from __future__ import annotations

import asyncio
import contextlib
import math
import signal

import grpc

from proto import worker_node_pb2_grpc
from worker.config import WorkerConfig
from worker.core.node import WorkerNode
from worker.grpc.servicer import WorkerControlServicer
from worker.telemetry.logging import configure_logging
from worker.telemetry.metrics import WorkerMetrics


async def run_worker() -> None:
    config = WorkerConfig.from_env()
    configure_logging(config.log_level)
    metrics = WorkerMetrics()
    node = WorkerNode(config=config, metrics=metrics)
    await node.start()

    server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(node), server)
    server.add_insecure_port(config.bind_target)
    await server.start()

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        asyncio.create_task(node.shutdown(max(1, math.ceil(config.graceful_shutdown_timeout_seconds))))

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown)

    try:
        await node.wait_for_stop()
    finally:
        with contextlib.suppress(Exception):
            await server.stop(grace=5)
        await node.close()


def main() -> None:
    asyncio.run(run_worker())


if __name__ == "__main__":
    main()
