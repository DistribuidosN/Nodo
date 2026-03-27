from __future__ import annotations

import asyncio
import logging

import grpc

from proto import worker_node_pb2, worker_node_pb2_grpc


class MockCoordinatorServicer(worker_node_pb2_grpc.CoordinatorCallbackServiceServicer):
    def __init__(self) -> None:
        self.progress_events: list[worker_node_pb2.ProgressEvent] = []
        self.results: list[worker_node_pb2.ExecutionResult] = []
        self.heartbeats: list[worker_node_pb2.HeartbeatRequest] = []
        self.result_event = asyncio.Event()

    async def ReportProgress(self, request, context):
        self.progress_events.append(request)
        return worker_node_pb2.ReportAck(accepted=True, message="progress recorded")

    async def ReportResult(self, request, context):
        self.results.append(request)
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
    server.add_insecure_port("127.0.0.1:50052")
    await server.start()
    logging.info("Mock coordinator listening on 127.0.0.1:50052")
    try:
        await server.wait_for_termination()
    finally:
        await server.stop(grace=3)


if __name__ == "__main__":
    asyncio.run(main())
