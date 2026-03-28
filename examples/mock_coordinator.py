from __future__ import annotations

import asyncio
import logging
import os

import grpc

from proto import worker_node_pb2, worker_node_pb2_grpc
from worker.grpc.security import build_server_credentials


class MockCoordinatorServicer(worker_node_pb2_grpc.CoordinatorCallbackServiceServicer):
    def __init__(self) -> None:
        self.progress_events: list[worker_node_pb2.ProgressEvent] = []
        self.results: list[worker_node_pb2.ExecutionResult] = []
        self.heartbeats: list[worker_node_pb2.HeartbeatRequest] = []
        self.progress_metadata: list[tuple[tuple[str, str], ...]] = []
        self.result_metadata: list[tuple[tuple[str, str], ...]] = []
        self.result_event = asyncio.Event()

    async def ReportProgress(self, request, context):
        self.progress_events.append(request)
        self.progress_metadata.append(tuple((item.key, item.value) for item in context.invocation_metadata()))
        return worker_node_pb2.ReportAck(accepted=True, message="progress recorded")

    async def ReportResult(self, request, context):
        self.results.append(request)
        self.result_metadata.append(tuple((item.key, item.value) for item in context.invocation_metadata()))
        self.result_event.set()
        return worker_node_pb2.ReportAck(accepted=True, message="result recorded")

    async def Heartbeat(self, request, context):
        self.heartbeats.append(request)
        return worker_node_pb2.HeartbeatReply(accepted=True, message="heartbeat recorded")


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    server = grpc.aio.server()
    servicer = MockCoordinatorServicer()
    worker_node_pb2_grpc.add_CoordinatorCallbackServiceServicer_to_server(servicer, server)
    bind_target = os.getenv("COORDINATOR_BIND_TARGET", "127.0.0.1:50052")
    server_credentials = build_server_credentials(
        cert_chain_file=os.getenv("COORDINATOR_GRPC_SERVER_CERT_FILE"),
        private_key_file=os.getenv("COORDINATOR_GRPC_SERVER_KEY_FILE"),
        client_ca_file=os.getenv("COORDINATOR_GRPC_SERVER_CLIENT_CA_FILE"),
        require_client_auth=os.getenv("COORDINATOR_GRPC_SERVER_REQUIRE_CLIENT_AUTH", "").lower() in {"1", "true", "yes"},
    )
    if server_credentials is None:
        server.add_insecure_port(bind_target)
    else:
        server.add_secure_port(bind_target, server_credentials)
    await server.start()
    logging.info("Mock coordinator listening on %s", bind_target)
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(grace=3)


if __name__ == "__main__":
    asyncio.run(main())
