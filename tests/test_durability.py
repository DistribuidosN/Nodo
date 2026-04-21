from __future__ import annotations

import asyncio
import socket
from io import BytesIO
from datetime import UTC, datetime

import pytest
from PIL import Image

from worker.config import WorkerConfig
from worker.core.node import WorkerNode
from worker.infrastructure.adapters.storage.local_storage_adapter import LocalStorageAdapter
from worker.infrastructure.adapters.image.pillow_adapter import PillowAdapter
from worker.core.state_store import EventSpoolStore, PendingTaskStore
from worker.execution.execution_manager import ExecutionManager
from worker.domain.exceptions import TaskCancelledError
from worker.domain.models import (
    ExecutionResultRecord,
    InputImageRef,
    OperationType,
    ProgressEventRecord,
    ResourceRequirements,
    Task,
    TaskState,
    TransformationSpec,
)
from worker.scheduler.cost_model import ServiceTimeEstimator
from worker.telemetry.metrics import WorkerMetrics


def build_config(tmp_path, metrics_port: int, health_port: int) -> WorkerConfig:
    return WorkerConfig(
        node_id="durability-node",
        bind_host="127.0.0.1",
        bind_port=50061,
        coordinator_target="127.0.0.1:59999",
        input_dir=tmp_path / "input",
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
        max_active_tasks=2,
        process_pool_workers=1,
        cpu_target=0.85,
        max_queue_size=16,
        queue_high_watermark=12,
        queue_low_watermark=8,
        min_free_memory_bytes=64 * 1024 * 1024,
        large_image_threshold_bytes=8 * 1024 * 1024,
        heartbeat_interval_seconds=0.2,
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
        log_level="INFO",
    )


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_payload() -> bytes:
    image = Image.new("RGB", (64, 64), color=(10, 120, 90))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def make_task(task_id: str, payload: bytes | None = None) -> Task:
    return Task(
        task_id=task_id,
        idempotency_key=task_id,
        priority=7,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=1,
        transforms=[TransformationSpec(operation=OperationType.GRAYSCALE)],
        input_image=InputImageRef(
            image_id=f"image-{task_id}",
            payload=payload,
            image_format="png",
            size_bytes=len(payload or b""),
            width=64,
            height=64,
        ),
        output_format="png",
        metadata={"node_id": "durability-node"},
    )


@pytest.mark.asyncio
async def test_worker_restores_pending_queue(tmp_path):
    config = build_config(tmp_path, metrics_port=free_port(), health_port=free_port())
    payload = build_payload()
    task = make_task("queued-task", payload=payload)
    task.next_eligible_at_monotonic = 10**12
    storage = LocalStorageAdapter()
    PendingTaskStore(storage, config.state_dir).upsert(task)

    storage = LocalStorageAdapter()
    processor = PillowAdapter(storage)
    node = WorkerNode(config=config, metrics=WorkerMetrics(), storage=storage, processor=processor)
    await node.start()
    state = await node.get_node_state()
    await node.close()

    assert state.queue_length == 1


def test_event_spool_store_roundtrip(tmp_path):
    storage = LocalStorageAdapter()
    spool = EventSpoolStore(storage, tmp_path / "state")
    progress = ProgressEventRecord(
        task_id="task-1",
        image_id="image-1",
        node_id="node-1",
        state=TaskState.RUNNING,
        progress_pct=50,
        attempt=1,
        queue_wait_ms=10,
        run_time_ms=20,
        message="halfway",
    )
    result = ExecutionResultRecord(
        task_id="task-1",
        image_id="image-1",
        node_id="node-1",
        state=TaskState.SUCCEEDED,
        attempt=1,
        output_path="/tmp/out.png",
        output_format="png",
        width=64,
        height=64,
        size_bytes=1024,
        finished_at=datetime.now(tz=UTC),
    )

    progress_id = spool.append("progress", progress)
    result_id = spool.append("result", result)
    loaded = spool.load_pending()

    assert {item.spool_id for item in loaded} == {progress_id, result_id}
    spool.ack(progress_id)
    remaining = spool.load_pending()
    assert len(remaining) == 1
    assert remaining[0].spool_id == result_id


def test_event_spool_store_supports_file_uri_backend(tmp_path):
    storage = LocalStorageAdapter()
    root_uri = (tmp_path / "shared-state").resolve().as_uri()
    spool = EventSpoolStore(storage, root_uri)
    progress = ProgressEventRecord(
        task_id="task-file",
        image_id="image-file",
        node_id="node-file",
        state=TaskState.RUNNING,
        progress_pct=25,
        attempt=1,
        queue_wait_ms=1,
        run_time_ms=2,
        message="queued",
    )

    spool_id = spool.append("progress", progress)
    loaded = spool.load_pending()

    assert len(loaded) == 1
    assert loaded[0].spool_id == spool_id


def test_process_image_honors_cancel_token(tmp_path):
    token_path = tmp_path / "cancel.token"
    token_path.write_text("cancel", encoding="utf-8")
    task = make_task("cancelled-task")
    task.cancel_token_path = str(token_path)
    task.transforms = [
        TransformationSpec(operation=OperationType.GRAYSCALE, params={"delay_ms": "50"}),
    ]

    adapter = PillowAdapter(LocalStorageAdapter())
    with pytest.raises(TaskCancelledError):
        adapter.process_task(task, str(tmp_path))


@pytest.mark.asyncio
async def test_execution_manager_kills_dedicated_process_on_cancel(tmp_path):
    config = build_config(tmp_path, metrics_port=free_port(), health_port=free_port())
    metrics = WorkerMetrics()
    estimator = ServiceTimeEstimator()
    payload = build_payload()
    task = make_task("process-cancel", payload=payload)
    task.metadata["execution_isolation"] = "process"
    task.transforms = [
        TransformationSpec(operation=OperationType.BLUR, params={"radius": "1.5", "delay_ms": "1200"}),
    ]
    task.metadata["node_id"] = config.node_id
    estimator.estimate(task)

    done = asyncio.Event()
    results: list[ExecutionResultRecord] = []

    async def emit_progress(_event):
        return None

    async def handle_result(_task, result, _queue_wait_ms, _run_time_ms):
        results.append(result)
        done.set()

    async def handle_failure(_task, error_code, error_message):
        raise AssertionError(f"unexpected failure: {error_code} {error_message}")

    async def release_resources(_task_id: str):
        return None

    manager = ExecutionManager(
        config=config,
        metrics=metrics,
        estimator=estimator,
        emit_progress=emit_progress,
        handle_result=handle_result,
        handle_failure=handle_failure,
        release_resources=release_resources,
    )

    await manager.start_task(
        task,
        ResourceRequirements(cpu_units=1.0, memory_bytes=8 * 1024 * 1024, io_units=0.1),
    )
    await asyncio.sleep(0.15)
    cancelled = await manager.cancel(task.task_id)
    await asyncio.wait_for(done.wait(), timeout=10)
    await manager.shutdown()

    assert cancelled is True
    assert results
    assert results[-1].state == TaskState.CANCELLED
