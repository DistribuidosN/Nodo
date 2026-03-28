from __future__ import annotations

import asyncio
import socket
from io import BytesIO
from datetime import UTC, datetime
from pathlib import Path

import grpc
import pytest
from PIL import Image
from google.protobuf.timestamp_pb2 import Timestamp

from coordinator.config import CoordinatorConfig
from coordinator.main import CoordinatorBusinessServicer, CoordinatorCallbackServicer
from coordinator.runtime import CoordinatorRuntime
from proto import imagenode_pb2, imagenode_pb2_grpc, worker_node_pb2, worker_node_pb2_grpc
from worker.config import WorkerConfig
from worker.core.node import WorkerNode
from worker.grpc.business_servicer import ImageNodeBusinessServicer
from worker.grpc.servicer import WorkerControlServicer
from worker.telemetry.metrics import WorkerMetrics


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_worker_config(node_id: str, tmp_path, worker_port: int, coordinator_port: int, metrics_port: int, health_port: int) -> WorkerConfig:
    return WorkerConfig(
        node_id=node_id,
        bind_host="127.0.0.1",
        bind_port=worker_port,
        coordinator_target=f"127.0.0.1:{coordinator_port}",
        metrics_host="127.0.0.1",
        metrics_port=metrics_port,
        health_host="127.0.0.1",
        health_port=health_port,
        output_dir=tmp_path / node_id / "out",
        state_dir=tmp_path / node_id / "state",
        max_active_tasks=2,
        process_pool_workers=1,
        thread_pool_workers=2,
        cpu_target=0.85,
        max_queue_size=16,
        queue_high_watermark=12,
        queue_low_watermark=8,
        min_free_memory_bytes=64 * 1024 * 1024,
        large_image_threshold_bytes=8 * 1024 * 1024,
        heartbeat_interval_seconds=0.3,
        report_queue_size=64,
        retry_base_ms=100,
        retry_max_ms=1000,
        score_deadline_window_seconds=30.0,
        score_aging_window_seconds=60.0,
        score_cost_window_ms=1500.0,
        scheduler_poll_seconds=0.02,
        dedupe_ttl_seconds=60,
        coordinator_reconnect_base_seconds=0.2,
        coordinator_reconnect_max_seconds=1.0,
        coordinator_failure_threshold=2,
        graceful_shutdown_timeout_seconds=2.0,
        process_cancel_grace_seconds=0.1,
        process_kill_timeout_seconds=1.0,
        log_level="INFO",
    )


def build_payload() -> bytes:
    image = Image.new("RGB", (160, 120), color=(90, 50, 210))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


async def wait_until(predicate, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    raise TimeoutError("condition not met before timeout")


@pytest.mark.asyncio
async def test_coordinator_dispatches_to_ready_worker_and_exposes_business_api(tmp_path):
    coordinator_port = free_port()
    worker1_port = free_port()
    worker2_port = free_port()
    metrics1_port = free_port()
    metrics2_port = free_port()
    health1_port = free_port()
    health2_port = free_port()

    runtime = CoordinatorRuntime(
        CoordinatorConfig(
            node_id="coord-test",
            bind_host="127.0.0.1",
            bind_port=coordinator_port,
            state_dir=Path(tmp_path) / "coordinator-state",
            workers={
                "worker-1": f"127.0.0.1:{worker1_port}",
                "worker-2": f"127.0.0.1:{worker2_port}",
            },
            dispatch_concurrency=2,
            dispatch_wait_seconds=0.05,
            status_poll_seconds=0.2,
            rpc_timeout_seconds=20.0,
            ownership_lease_seconds=3.0,
            log_level="INFO",
        )
    )
    await runtime.start()

    coordinator_server = grpc.aio.server()
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(CoordinatorBusinessServicer(runtime), coordinator_server)
    worker_node_pb2_grpc.add_CoordinatorCallbackServiceServicer_to_server(CoordinatorCallbackServicer(runtime), coordinator_server)
    coordinator_server.add_insecure_port(f"127.0.0.1:{coordinator_port}")
    await coordinator_server.start()

    worker1 = WorkerNode(build_worker_config("worker-1", tmp_path, worker1_port, coordinator_port, metrics1_port, health1_port), WorkerMetrics())
    worker2 = WorkerNode(build_worker_config("worker-2", tmp_path, worker2_port, coordinator_port, metrics2_port, health2_port), WorkerMetrics())
    await worker1.start()
    await worker2.start()

    worker1_server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(worker1), worker1_server)
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(ImageNodeBusinessServicer(worker1, WorkerMetrics()), worker1_server)
    worker1_server.add_insecure_port(f"127.0.0.1:{worker1_port}")
    await worker1_server.start()

    worker2_server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(worker2), worker2_server)
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(ImageNodeBusinessServicer(worker2, WorkerMetrics()), worker2_server)
    worker2_server.add_insecure_port(f"127.0.0.1:{worker2_port}")
    await worker2_server.start()

    await wait_until(lambda: runtime.ready_workers >= 2, timeout=8.0)

    drain_channel = grpc.aio.insecure_channel(f"127.0.0.1:{worker1_port}")
    drain_stub = worker_node_pb2_grpc.WorkerControlServiceStub(drain_channel)
    drain_response = await drain_stub.DrainNode(worker_node_pb2.DrainNodeRequest(reject_new_tasks=True))
    assert drain_response.accepted is True
    await wait_until(
        lambda: (runtime._workers["worker-1"].status is not None and not runtime._workers["worker-1"].status.accepting_tasks),
        timeout=5.0,
    )

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{coordinator_port}")
    stub = imagenode_pb2_grpc.ImageNodeServiceStub(channel)
    response = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=build_payload(),
            file_name="via-coordinator.png",
            filters=["grayscale", "resize:80x60"],
        )
    )

    assert response.success is True
    processed = Image.open(BytesIO(response.image_data))
    assert processed.size == (80, 60)
    record = runtime.get_record("via-coordinator.png")
    assert record is not None
    assert record.node_id == "worker-2"
    assert runtime._workers["worker-2"].dispatches == 1

    cached_response = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=build_payload(),
            file_name="via-coordinator.png",
            filters=["grayscale", "resize:80x60"],
        )
    )
    assert cached_response.success is True
    assert cached_response.result_path == response.result_path
    assert runtime._workers["worker-2"].dispatches == 1

    path_response = await stub.FindPathByName(imagenode_pb2.FileNameRequest(file_name="via-coordinator.png"))
    assert path_response.success is True
    assert path_response.result_path == response.result_path

    metrics_response = await stub.GetMetrics(imagenode_pb2.EmptyRequest())
    assert metrics_response.statistics["ready_workers"] >= 1.0

    await channel.close()
    await drain_channel.close()
    await worker1_server.stop(grace=3)
    await worker2_server.stop(grace=3)
    await worker1.close()
    await worker2.close()
    await coordinator_server.stop(grace=3)
    await runtime.close()
