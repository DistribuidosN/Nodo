from __future__ import annotations

import socket
from io import BytesIO

import grpc
import pytest
from PIL import Image

from proto import imagenode_pb2, imagenode_pb2_grpc, worker_node_pb2, worker_node_pb2_grpc
from worker.config import WorkerConfig
from worker.core.node import WorkerNode
from worker.grpc.business_servicer import ImageNodeBusinessServicer
from worker.telemetry.metrics import WorkerMetrics


COORDINATOR_CALLBACK_SERVICE = worker_node_pb2_grpc.add_CoordinatorCallbackServiceServicer_to_server


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_config(
    tmp_path,
    worker_port: int,
    coordinator_port: int,
    metrics_port: int,
    health_port: int,
    **overrides,
) -> WorkerConfig:
    values = {
        "node_id": "imagenode-test",
        "bind_host": "127.0.0.1",
        "bind_port": worker_port,
        "coordinator_target": f"127.0.0.1:{coordinator_port}",
        "metrics_host": "127.0.0.1",
        "metrics_port": metrics_port,
        "health_host": "127.0.0.1",
        "health_port": health_port,
        "output_dir": tmp_path / "out",
        "state_dir": tmp_path / "state",
        "max_active_tasks": 2,
        "process_pool_workers": 1,
        "thread_pool_workers": 2,
        "cpu_target": 0.85,
        "max_queue_size": 16,
        "queue_high_watermark": 12,
        "queue_low_watermark": 8,
        "min_free_memory_bytes": 64 * 1024 * 1024,
        "large_image_threshold_bytes": 8 * 1024 * 1024,
        "heartbeat_interval_seconds": 0.5,
        "report_queue_size": 64,
        "retry_base_ms": 100,
        "retry_max_ms": 1000,
        "score_deadline_window_seconds": 30.0,
        "score_aging_window_seconds": 60.0,
        "score_cost_window_ms": 1500.0,
        "scheduler_poll_seconds": 0.02,
        "dedupe_ttl_seconds": 60,
        "coordinator_reconnect_base_seconds": 0.2,
        "coordinator_reconnect_max_seconds": 1.0,
        "coordinator_failure_threshold": 2,
        "graceful_shutdown_timeout_seconds": 2.0,
        "process_cancel_grace_seconds": 0.1,
        "process_kill_timeout_seconds": 1.0,
        "log_level": "INFO",
    }
    values.update(overrides)
    return WorkerConfig(**values)


def build_payload(color=(90, 50, 210), size=(160, 120)) -> bytes:
    image = Image.new("RGB", size, color=color)
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


class MockCoordinator(worker_node_pb2_grpc.CoordinatorCallbackServiceServicer):
    async def ReportProgress(self, request, context):
        return worker_node_pb2.ReportAck(accepted=True, message="ok")

    async def ReportResult(self, request, context):
        return worker_node_pb2.ReportAck(accepted=True, message="ok")

    async def Heartbeat(self, request, context):
        return worker_node_pb2.HeartbeatReply(accepted=True, message="ok")


@pytest.mark.asyncio
async def test_imagenode_service_processes_unary_batch_stream_and_search(tmp_path):
    worker_port = free_port()
    coordinator_port = free_port()
    metrics_port = free_port()
    health_port = free_port()

    coordinator = MockCoordinator()
    coordinator_server = grpc.aio.server()
    COORDINATOR_CALLBACK_SERVICE(coordinator, coordinator_server)
    coordinator_server.add_insecure_port(f"127.0.0.1:{coordinator_port}")
    await coordinator_server.start()

    config = build_config(tmp_path, worker_port, coordinator_port, metrics_port, health_port)
    metrics = WorkerMetrics()
    node = WorkerNode(config=config, metrics=metrics)
    await node.start()

    worker_server = grpc.aio.server()
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(
        ImageNodeBusinessServicer(node=node, metrics=metrics, stream_concurrency=2),
        worker_server,
    )
    worker_server.add_insecure_port(f"127.0.0.1:{worker_port}")
    await worker_server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{worker_port}")
    stub = imagenode_pb2_grpc.ImageNodeServiceStub(channel)

    unary = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=build_payload(),
            file_name="demo.png",
            filters=["grayscale", "resize:80x60"],
        )
    )
    assert unary.success is True
    assert unary.file_name == "demo.png"
    assert unary.result_path
    unary_image = Image.open(BytesIO(unary.image_data))
    assert unary_image.size == (80, 60)

    paths = await stub.GetProcessedFilePaths(imagenode_pb2.EmptyRequest())
    assert unary.result_path in paths.paths

    find_path = await stub.FindPathByName(imagenode_pb2.FileNameRequest(file_name="demo.png"))
    assert find_path.success is True
    assert find_path.result_path == unary.result_path

    find_image = await stub.FindImageByName(imagenode_pb2.FileNameRequest(file_name="demo.png"))
    assert find_image.success is True
    assert find_image.result_path == unary.result_path

    all_images = await stub.GetProcessedImages(imagenode_pb2.EmptyRequest())
    assert all_images.images

    batch = await stub.ProcessBatch(
        imagenode_pb2.BatchRequest(
            requests=[
                imagenode_pb2.ProcessRequest(
                    image_data=build_payload(color=(10, 120, 80)),
                    file_name="batch-a.png",
                    filters=["resize:40x30"],
                ),
                imagenode_pb2.ProcessRequest(
                    image_data=build_payload(color=(200, 120, 80)),
                    file_name="batch-b.png",
                    filters=["rotate:90"],
                ),
            ]
        )
    )
    assert batch.all_success is True
    assert len(batch.responses) == 2
    assert all(item.success for item in batch.responses)

    async def chunk_stream():
        payload = build_payload(size=(200, 150))
        midpoint = len(payload) // 2
        yield imagenode_pb2.ImageChunk(
            chunk_data=payload[:midpoint],
            file_name="large.png",
            filters=["resize:50x40"],
        )
        yield imagenode_pb2.ImageChunk(chunk_data=payload[midpoint:])

    upload = await stub.UploadLargeImage(chunk_stream())
    assert upload.success is True
    uploaded_image = Image.open(BytesIO(upload.image_data))
    assert uploaded_image.size == (50, 40)

    async def request_stream():
        yield imagenode_pb2.ProcessRequest(
            image_data=build_payload(color=(0, 0, 0), size=(60, 40)),
            file_name="stream-a.png",
            filters=["resize:30x20"],
        )
        yield imagenode_pb2.ProcessRequest(
            image_data=build_payload(color=(255, 0, 0), size=(90, 50)),
            file_name="stream-b.png",
            filters=["grayscale"],
        )

    streamed = [item async for item in stub.StreamBatchProcess(request_stream())]
    assert len(streamed) == 2
    assert all(item.success for item in streamed)
    assert {item.file_name for item in streamed} == {"stream-a.png", "stream-b.png"}

    too_many_filters = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=build_payload(),
            file_name="too-many.png",
            filters=["grayscale"] * 13,
        )
    )
    assert too_many_filters.success is False
    assert "too many filters" in too_many_filters.message.lower()

    health = await stub.HealthCheck(imagenode_pb2.EmptyRequest())
    assert health.is_alive is True
    assert health.node_id == "imagenode-test"

    metrics_response = await stub.GetMetrics(imagenode_pb2.EmptyRequest())
    assert "queue_length" in metrics_response.statistics
    assert "active_tasks" in metrics_response.statistics

    await channel.close()
    await worker_server.stop(grace=3)
    await node.close()
    await coordinator_server.stop(grace=3)


@pytest.mark.asyncio
async def test_imagenode_service_enforces_payload_and_batch_limits(tmp_path):
    worker_port = free_port()
    coordinator_port = free_port()
    metrics_port = free_port()
    health_port = free_port()

    coordinator = MockCoordinator()
    coordinator_server = grpc.aio.server()
    COORDINATOR_CALLBACK_SERVICE(coordinator, coordinator_server)
    coordinator_server.add_insecure_port(f"127.0.0.1:{coordinator_port}")
    await coordinator_server.start()

    config = build_config(
        tmp_path,
        worker_port,
        coordinator_port,
        metrics_port,
        health_port,
        max_request_bytes=256,
        max_batch_size=2,
    )
    metrics = WorkerMetrics()
    node = WorkerNode(config=config, metrics=metrics)
    await node.start()

    worker_server = grpc.aio.server()
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(
        ImageNodeBusinessServicer(node=node, metrics=metrics, stream_concurrency=2),
        worker_server,
    )
    worker_server.add_insecure_port(f"127.0.0.1:{worker_port}")
    await worker_server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{worker_port}")
    stub = imagenode_pb2_grpc.ImageNodeServiceStub(channel)

    oversized = await stub.ProcessToData(
        imagenode_pb2.ProcessRequest(
            image_data=b"x" * 512,
            file_name="too-large.bin",
            filters=[],
        )
    )
    assert oversized.success is False
    assert "payload too large" in oversized.message.lower()

    batch = await stub.ProcessBatch(
        imagenode_pb2.BatchRequest(
            requests=[
                imagenode_pb2.ProcessRequest(image_data=build_payload(), file_name="a.png", filters=[]),
                imagenode_pb2.ProcessRequest(image_data=build_payload(), file_name="b.png", filters=[]),
                imagenode_pb2.ProcessRequest(image_data=build_payload(), file_name="c.png", filters=[]),
            ]
        )
    )
    assert batch.all_success is False
    assert batch.responses
    assert "batch too large" in batch.responses[0].message.lower()

    await channel.close()
    await worker_server.stop(grace=3)
    await node.close()
    await coordinator_server.stop(grace=3)
