from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from worker.config import WorkerConfig
from worker.models.types import InputImageRef, OperationType, Task, TransformationSpec
from worker.scheduler.priority_queue import TaskPriorityQueue
from worker.scheduler.scoring import TaskScorer


def make_config(tmp_path):
    return WorkerConfig(
        node_id="test-node",
        bind_host="127.0.0.1",
        bind_port=50051,
        coordinator_target="127.0.0.1:50052",
        metrics_host="127.0.0.1",
        metrics_port=9101,
        health_host="127.0.0.1",
        health_port=18081,
        output_dir=tmp_path,
        state_dir=tmp_path / "state",
        max_active_tasks=2,
        process_pool_workers=1,
        thread_pool_workers=2,
        cpu_target=0.85,
        max_queue_size=32,
        queue_high_watermark=24,
        queue_low_watermark=16,
        min_free_memory_bytes=64 * 1024 * 1024,
        large_image_threshold_bytes=8 * 1024 * 1024,
        heartbeat_interval_seconds=1.0,
        report_queue_size=32,
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


def make_task(task_id: str, priority: int, deadline_offset_s: int | None = None) -> Task:
    deadline = datetime.now(tz=UTC) + timedelta(seconds=deadline_offset_s) if deadline_offset_s is not None else None
    return Task(
        task_id=task_id,
        idempotency_key=task_id,
        priority=priority,
        created_at=datetime.now(tz=UTC),
        deadline=deadline,
        max_retries=1,
        transforms=[TransformationSpec(operation=OperationType.GRAYSCALE)],
        input_image=InputImageRef(image_id=task_id, size_bytes=1024, width=100, height=100),
        output_format="png",
    )


@pytest.mark.asyncio
async def test_priority_queue_prefers_higher_priority(tmp_path):
    queue = TaskPriorityQueue(TaskScorer(make_config(tmp_path)))
    low = make_task("low", priority=2)
    high = make_task("high", priority=9)
    await queue.enqueue(low)
    await asyncio.sleep(0.01)
    await queue.enqueue(high)

    selected, _ = await queue.pop_ready()
    assert selected is not None
    assert selected.task_id == "high"


@pytest.mark.asyncio
async def test_priority_queue_honors_deadline_urgency(tmp_path):
    queue = TaskPriorityQueue(TaskScorer(make_config(tmp_path)))
    relaxed = make_task("relaxed", priority=5, deadline_offset_s=30)
    urgent = make_task("urgent", priority=5, deadline_offset_s=2)
    await queue.enqueue(relaxed)
    await queue.enqueue(urgent)

    selected, _ = await queue.pop_ready()
    assert selected is not None
    assert selected.task_id == "urgent"
