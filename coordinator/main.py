from __future__ import annotations

import asyncio
import contextlib
import math
import signal

import grpc

from coordinator.config import CoordinatorConfig
from coordinator.health import CoordinatorHealthServer
from coordinator.runtime import CoordinatorRuntime
from proto import imagenode_pb2, imagenode_pb2_grpc, worker_node_pb2_grpc
from worker.grpc.security import build_server_credentials
from worker.telemetry.logging import configure_logging


class CoordinatorBusinessServicer(imagenode_pb2_grpc.ImageNodeServiceServicer):
    def __init__(self, runtime: CoordinatorRuntime) -> None:
        self._runtime = runtime

    async def ProcessToPath(self, request, context):
        return await self._runtime.process_to_path(request)

    async def ProcessToData(self, request, context):
        return await self._runtime.process_to_data(request)

    async def HealthCheck(self, request, context):
        return await self._runtime.health_check()

    async def GetMetrics(self, request, context):
        return await self._runtime.get_metrics()

    async def UploadLargeImage(self, request_iterator, context):
        return await self._runtime.upload_large_image(request_iterator)

    async def StreamBatchProcess(self, request_iterator, context):
        async for item in self._runtime.stream_batch(request_iterator):
            yield item

    async def GetProcessedFilePaths(self, request, context):
        return imagenode_pb2.PathListResponse(paths=self._runtime.get_processed_file_paths())

    async def FindPathByName(self, request, context):
        path = await self._runtime.find_path_by_name(request.file_name)
        if path is None:
            return imagenode_pb2.PathResponse(success=False, message="file not found")
        return imagenode_pb2.PathResponse(result_path=path, success=True, message="ok")

    async def GetProcessedImages(self, request, context):
        return imagenode_pb2.DataListResponse(images=await self._runtime.get_processed_images())

    async def FindImageByName(self, request, context):
        image_data = await self._runtime.find_image_by_name(request.file_name)
        if image_data is None:
            return imagenode_pb2.DataResponse(success=False, file_name=request.file_name, message="file not found")
        record = self._runtime.get_record(request.file_name)
        return imagenode_pb2.DataResponse(
            image_data=image_data,
            success=True,
            file_name=request.file_name,
            result_path=record.result_path if record else "",
            message="ok",
        )

    async def ProcessBatch(self, request, context):
        return await self._runtime.process_batch(request.requests)


class CoordinatorCallbackServicer(worker_node_pb2_grpc.CoordinatorCallbackServiceServicer):
    def __init__(self, runtime: CoordinatorRuntime) -> None:
        self._runtime = runtime

    async def ReportProgress(self, request, context):
        return await self._runtime.record_progress(request)

    async def ReportResult(self, request, context):
        return await self._runtime.record_result(request)

    async def Heartbeat(self, request, context):
        return await self._runtime.record_heartbeat(request)


async def run_coordinator() -> None:
    config = CoordinatorConfig.from_env()
    configure_logging(config.log_level)
    runtime = CoordinatorRuntime(config)
    await runtime.start()
    health_server = CoordinatorHealthServer(config.health_host, config.health_port, runtime.current_health)
    health_server.start()

    server = grpc.aio.server()
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(CoordinatorBusinessServicer(runtime), server)
    worker_node_pb2_grpc.add_CoordinatorCallbackServiceServicer_to_server(CoordinatorCallbackServicer(runtime), server)

    credentials = build_server_credentials(
        cert_chain_file=config.grpc_server_cert_file,
        private_key_file=config.grpc_server_key_file,
        client_ca_file=config.grpc_server_client_ca_file,
        require_client_auth=config.grpc_server_require_client_auth,
    )
    if credentials is None:
        server.add_insecure_port(config.bind_target)
    else:
        server.add_secure_port(config.bind_target, credentials)
    await server.start()

    loop = asyncio.get_running_loop()

    async def _shutdown() -> None:
        await runtime.close()
        health_server.stop()
        await server.stop(grace=max(1, math.ceil(config.dispatch_wait_seconds * 10)))

    def _request_shutdown() -> None:
        asyncio.create_task(_shutdown())

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown)

    try:
        await server.wait_for_termination()
    finally:
        await runtime.close()
        health_server.stop()
        await server.stop(grace=3)


def main() -> None:
    asyncio.run(run_coordinator())
