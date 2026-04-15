from __future__ import annotations

import asyncio
import contextlib
import math
import signal

import grpc

from proto import imagenode_pb2_grpc, orchestrator_pb2_grpc, worker_node_pb2_grpc
from worker.config import WorkerConfig
from worker.core.worker_runtime import WorkerNode
from worker.grpc.image_node_service import ImageNodeBusinessServicer
from worker.grpc.orchestrator_worker_node_service import OrchestratorWorkerNodeServicer
from worker.grpc.security import build_server_credentials
from worker.grpc.worker_control_service import WorkerControlServicer
from worker.telemetry.logging import configure_logging
from worker.telemetry.metrics import WorkerMetrics
from worker.telemetry.tracing import configure_tracing, shutdown_tracing


async def run_worker_server() -> None:
    config = WorkerConfig.from_env()
    configure_logging(config.log_level)
    configure_tracing(config)
    metrics = WorkerMetrics()
    node = WorkerNode(config=config, metrics=metrics)
    await node.start()

    server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(node), server)  # type: ignore[attr-defined]
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(  # type: ignore[attr-defined]
        ImageNodeBusinessServicer(node=node, metrics=metrics, stream_concurrency=config.max_active_tasks),
        server,
    )
    orchestrator_pb2_grpc.add_WorkerNodeServicer_to_server(  # type: ignore[attr-defined]
        OrchestratorWorkerNodeServicer(node=node, metrics=metrics),
        server,
    )
    server_credentials = build_server_credentials(
        cert_chain_file=config.grpc_server_cert_file,
        private_key_file=config.grpc_server_key_file,
        client_ca_file=config.grpc_server_client_ca_file,
        require_client_auth=config.grpc_server_require_client_auth,
    )
    if server_credentials is None:
        server.add_insecure_port(config.bind_target)
    else:
        server.add_secure_port(config.bind_target, server_credentials)
    await server.start()

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        node.shutdown(max(1, math.ceil(config.graceful_shutdown_timeout_seconds)))

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown)

    try:
        await node.wait_for_stop()
    finally:
        with contextlib.suppress(Exception):
            await server.stop(grace=5)
        await node.close()
        shutdown_tracing()


def main() -> None:
    asyncio.run(run_worker_server())


if __name__ == "__main__":
    main()
