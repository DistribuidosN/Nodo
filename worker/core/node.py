from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from datetime import UTC, datetime

from worker.config import WorkerConfig
from worker.core.state_store import PendingTaskStore, StateStore
from worker.core.storage import StorageClient
from worker.execution.execution_manager import ExecutionManager
from worker.execution.resource_manager import ResourceManager
from worker.grpc.reporter import CoordinatorReporter
from worker.models.types import (
    ExecutionResultRecord,
    HealthState,
    NodeHealth,
    NodeMode,
    NodeState,
    ProgressEventRecord,
    RetryPolicy,
    Task,
    TaskState,
)
from worker.scheduler.cost_model import ServiceTimeEstimator
from worker.scheduler.priority_queue import TaskPriorityQueue
from worker.scheduler.scoring import TaskScorer
from worker.telemetry.health import HealthServer
from worker.telemetry.metrics import WorkerMetrics
from worker.telemetry.tracing import copy_internal_trace_metadata


PROTO_STATUS = {
    TaskState.QUEUED: 1,
    TaskState.ADMITTED: 2,
    TaskState.RUNNING: 3,
    TaskState.RETRY_SCHEDULED: 4,
    TaskState.SUCCEEDED: 5,
    TaskState.FAILED: 6,
    TaskState.CANCELLED: 7,
    TaskState.REJECTED: 8,
}


class WorkerNode:
    def __init__(self, config: WorkerConfig, metrics: WorkerMetrics) -> None:
        self._config = config
        self._metrics = metrics
        self._logger = logging.getLogger("worker.node")
        self._scorer = TaskScorer(config)
        self._estimator = ServiceTimeEstimator()
        self._queue = TaskPriorityQueue(self._scorer)
        self._resource_manager = ResourceManager(config)
        self._retry_policy = RetryPolicy(base_delay_ms=config.retry_base_ms, max_delay_ms=config.retry_max_ms)
        self._mode = NodeMode.ACTIVE
        self._accepting_tasks = True
        self._stop_event = asyncio.Event()
        self._scheduler_task: asyncio.Task | None = None
        self._health_task: asyncio.Task | None = None
        self._tasks: dict[str, Task] = {}
        self._results: dict[str, ExecutionResultRecord] = {}
        self._idempotency_index: dict[str, str] = {}
        self._completed_at: dict[str, float] = {}
        self._result_waiters: dict[str, list[asyncio.Future[ExecutionResultRecord]]] = {}
        self._task_lock = asyncio.Lock()
        self._storage = StorageClient.from_config(config)
        self._state_root_uri = self._build_state_root_uri()
        self._state_store = StateStore(self._storage, self._state_root_uri)
        self._pending_store = PendingTaskStore(self._storage, self._state_root_uri)
        self._latest_health = NodeHealth(
            node_id=config.node_id,
            state=HealthState.NOT_READY,
            live=True,
            ready=False,
            coordinator_connected=False,
            queue_length=0,
            active_tasks=0,
            message="worker booting",
        )
        self._health_server = HealthServer(config.health_host, config.health_port, self.current_health)
        self._reporter = CoordinatorReporter(
            config=config,
            metrics=metrics,
            status_provider=self.get_node_state,
            storage=self._storage,
            state_root_uri=self._state_root_uri,
        )
        self._execution_manager = ExecutionManager(
            config=config,
            metrics=metrics,
            estimator=self._estimator,
            emit_progress=self._emit_progress,
            handle_result=self._handle_result,
            handle_failure=self._handle_failure,
            release_resources=self._resource_manager.release,
            storage=self._storage,
        )

    async def start(self) -> None:
        self._validate_storage_requirements()
        self._config.output_dir.mkdir(parents=True, exist_ok=True)
        self._config.state_dir.mkdir(parents=True, exist_ok=True)
        self._restore_completed_state()
        await self._restore_pending_tasks()
        self._metrics.start_server(self._config.metrics_host, self._config.metrics_port)
        self._health_server.start()
        await self._reporter.start()
        self._scheduler_task = asyncio.create_task(self._scheduler_loop(), name="worker-scheduler")
        self._health_task = asyncio.create_task(self._health_loop(), name="worker-health")

    async def close(self) -> None:
        self._stop_event.set()
        if self._health_task is not None:
            self._health_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._health_task
        if self._scheduler_task is not None:
            self._scheduler_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._scheduler_task
        await self._execution_manager.shutdown()
        await self._reporter.stop()
        self._health_server.stop()

    async def submit_task(self, task: Task) -> dict:
        async with self._task_lock:
            self._cleanup_completed_cache()
            if not self._accepting_tasks:
                self._metrics.tasks_rejected.inc()
                return self._submit_reply(False, False, task.task_id, TaskState.REJECTED, 0, "worker is draining or shutting down")

            if existing_task_id := self._idempotency_index.get(task.idempotency_key):
                existing_result = self._results.get(existing_task_id)
                existing_state = existing_result.state if existing_result is not None else TaskState.QUEUED
                return self._submit_reply(True, True, existing_task_id, existing_state, 0, "duplicate task reused existing idempotency key")

            queue_size = await self._queue.qsize()
            if queue_size >= self._config.max_queue_size or queue_size >= self._config.queue_high_watermark:
                self._metrics.tasks_rejected.inc()
                return self._submit_reply(False, False, task.task_id, TaskState.REJECTED, 0, "queue high watermark reached")

            snapshot = await self._resource_manager.snapshot(queue_length=queue_size, active_tasks=self._execution_manager.active_count)
            if (
                task.input_image.size_bytes >= self._config.large_image_threshold_bytes
                and snapshot.available_memory_bytes < self._config.min_free_memory_bytes * 2
            ):
                self._metrics.tasks_rejected.inc()
                return self._submit_reply(False, False, task.task_id, TaskState.REJECTED, 0, "insufficient memory headroom for large image")

            self._estimator.estimate(task)
            task.metadata["node_id"] = self._config.node_id
            task.enqueued_at_monotonic = time.monotonic()
            task.status = TaskState.QUEUED
            self._tasks[task.task_id] = task
            self._idempotency_index[task.idempotency_key] = task.task_id
            position = await self._queue.enqueue(task)
            self._pending_store.upsert(task)

        await self._emit_progress(
            ProgressEventRecord(
                task_id=task.task_id,
                image_id=task.input_image.image_id,
                node_id=self._config.node_id,
                state=TaskState.QUEUED,
                progress_pct=0,
                attempt=task.attempt,
                queue_wait_ms=0,
                run_time_ms=0,
                message="task accepted into local priority queue",
                metadata=self._trace_metadata(task),
            )
        )
        return self._submit_reply(True, False, task.task_id, TaskState.QUEUED, position, "")

    async def cancel_task(self, task_id: str, reason: str) -> dict:
        queued_task = await self._queue.remove(task_id)
        if queued_task is not None:
            queued_task.status = TaskState.CANCELLED
            self._pending_store.remove(task_id)
            self._metrics.tasks_cancelled.inc()
            await self._emit_progress(
                ProgressEventRecord(
                    task_id=task_id,
                    image_id=queued_task.input_image.image_id,
                    node_id=self._config.node_id,
                    state=TaskState.CANCELLED,
                    progress_pct=0,
                    attempt=queued_task.attempt,
                    queue_wait_ms=0,
                    run_time_ms=0,
                    message=reason or "task cancelled while queued",
                    metadata=self._trace_metadata(queued_task),
                )
            )
            return {"accepted": True, "status": PROTO_STATUS[TaskState.CANCELLED], "message": "queued task cancelled"}

        cancelled = await self._execution_manager.cancel(task_id)
        if cancelled:
            self._metrics.tasks_cancelled.inc()
            return {"accepted": True, "status": PROTO_STATUS[TaskState.CANCELLED], "message": "running task cancellation requested"}
        return {"accepted": False, "status": 0, "message": "task not found"}

    async def drain(self, reject_new_tasks: bool) -> dict:
        self._mode = NodeMode.DRAINING
        self._accepting_tasks = not reject_new_tasks
        return {"accepted": True, "message": "worker switched to draining mode"}

    async def shutdown(self, grace_period_seconds: int) -> dict:
        self._mode = NodeMode.SHUTTING_DOWN
        self._accepting_tasks = False

        async def _close_later() -> None:
            await asyncio.sleep(grace_period_seconds)
            self._stop_event.set()

        asyncio.create_task(_close_later())
        return {"accepted": True, "message": "shutdown scheduled"}

    async def get_node_state(self) -> NodeState:
        queue_length = await self._queue.qsize()
        snapshot = await self._resource_manager.snapshot(queue_length=queue_length, active_tasks=self._execution_manager.active_count)
        counts = await self._queue.task_counts_by_priority()
        self._metrics.update_resources(snapshot)
        self._metrics.set_tasks_by_priority(counts)
        state = NodeState(
            node_id=self._config.node_id,
            mode=self._mode,
            accepting_tasks=self._accepting_tasks,
            queue_length=queue_length,
            active_tasks=self._execution_manager.active_count,
            capacity_effective=snapshot.capacity_effective,
            cpu_utilization=snapshot.cpu_utilization,
            memory_utilization=snapshot.memory_utilization,
            gpu_utilization=snapshot.gpu_utilization,
            io_utilization=snapshot.io_utilization,
            available_memory_bytes=snapshot.available_memory_bytes,
            reserved_memory_bytes=snapshot.reserved_memory_bytes,
            timestamp=datetime.now(tz=UTC),
        )
        self._update_health_from_state(state)
        return state

    async def wait_for_stop(self) -> None:
        await self._stop_event.wait()

    def current_health(self) -> NodeHealth:
        return self._latest_health

    async def wait_for_result(self, task_id: str, timeout_seconds: float | None = None) -> ExecutionResultRecord:
        existing = self._results.get(task_id)
        if existing is not None and existing.state in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}:
            return existing

        loop = asyncio.get_running_loop()
        future: asyncio.Future[ExecutionResultRecord] = loop.create_future()
        self._result_waiters.setdefault(task_id, []).append(future)
        try:
            if timeout_seconds is None:
                return await future
            return await asyncio.wait_for(future, timeout=timeout_seconds)
        finally:
            waiters = self._result_waiters.get(task_id, [])
            if future in waiters:
                waiters.remove(future)
            if not waiters:
                self._result_waiters.pop(task_id, None)

    def list_completed_results(self) -> list[ExecutionResultRecord]:
        completed = [item for item in self._results.values() if item.state in {TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED}]
        return sorted(completed, key=lambda item: item.finished_at or datetime.min.replace(tzinfo=UTC), reverse=True)

    def find_result_by_source_name(self, file_name: str) -> ExecutionResultRecord | None:
        normalized = file_name.strip().lower()
        for result in self.list_completed_results():
            if result.metadata.get("source_file_name", "").lower() == normalized:
                return result
            if result.output_path and result.output_path.rsplit("/", 1)[-1].lower() == normalized:
                return result
        return None

    def read_output_bytes(self, result: ExecutionResultRecord) -> bytes:
        if not result.output_path:
            raise FileNotFoundError("result does not contain an output path")
        return self._storage.read_bytes(result.output_path)

    async def _scheduler_loop(self) -> None:
        while not self._stop_event.is_set():
            if self._mode == NodeMode.SHUTTING_DOWN:
                await asyncio.sleep(self._config.scheduler_poll_seconds)
                continue

            task, wait_hint = await self._queue.pop_ready()
            if task is None:
                await self._queue.wait_for_items(timeout=wait_hint or self._config.scheduler_poll_seconds)
                continue

            queue_length = await self._queue.qsize()
            can_run, requirements, snapshot = await self._resource_manager.can_run(
                task,
                queue_length=queue_length,
                active_tasks=self._execution_manager.active_count,
            )
            self._metrics.update_resources(snapshot)
            if not can_run:
                task.next_eligible_at_monotonic = time.monotonic() + self._config.scheduler_poll_seconds
                await self._queue.enqueue(task)
                self._pending_store.upsert(task)
                await asyncio.sleep(self._config.scheduler_poll_seconds)
                continue

            await self._resource_manager.reserve(task.task_id, requirements)
            await self._execution_manager.start_task(task, requirements)
            self._pending_store.remove(task.task_id)

    async def _emit_progress(self, event: ProgressEventRecord) -> None:
        await self._reporter.report_progress(event)

    async def _handle_result(
        self,
        task: Task,
        result: ExecutionResultRecord,
        queue_wait_ms: int,
        run_time_ms: int,
    ) -> None:
        task.status = result.state
        self._pending_store.remove(task.task_id)
        self._results[task.task_id] = result
        self._completed_at[task.task_id] = time.monotonic()
        self._metrics.queue_wait_ms.observe(queue_wait_ms)
        if task.deadline and result.finished_at and result.finished_at > task.deadline:
            self._metrics.deadline_miss_total.inc()

        if result.state == TaskState.CANCELLED:
            self._metrics.tasks_cancelled.inc()
        else:
            self._metrics.success_total.inc()

        self._state_store.append_result(result, task.idempotency_key)
        await self._emit_progress(
            ProgressEventRecord(
                task_id=task.task_id,
                image_id=task.input_image.image_id,
                node_id=self._config.node_id,
                state=result.state,
                progress_pct=100,
                attempt=task.attempt,
                queue_wait_ms=queue_wait_ms,
                run_time_ms=run_time_ms,
                message="task finished",
                metadata=self._trace_metadata(task),
            )
        )
        await self._reporter.report_result(result)
        self._resolve_result_waiters(task.task_id, result)

    async def _handle_failure(self, task: Task, error_code: str, error_message: str) -> None:
        task.last_error = error_message
        if task.attempt < task.max_retries and self._accepting_tasks:
            task.status = TaskState.RETRY_SCHEDULED
            next_attempt = task.attempt + 1
            task.next_eligible_at_monotonic = time.monotonic() + self._retry_policy.next_delay_seconds(next_attempt)
            self._metrics.retry_total.inc()
            await self._emit_progress(
                ProgressEventRecord(
                    task_id=task.task_id,
                    image_id=task.input_image.image_id,
                    node_id=self._config.node_id,
                    state=TaskState.RETRY_SCHEDULED,
                    progress_pct=0,
                    attempt=next_attempt,
                    queue_wait_ms=0,
                    run_time_ms=0,
                    message=f"retry scheduled after {error_code}",
                    metadata=self._trace_metadata(task),
                )
            )
            await self._queue.enqueue(task)
            self._pending_store.upsert(task)
            return

        task.status = TaskState.FAILED
        self._pending_store.remove(task.task_id)
        result = ExecutionResultRecord(
            task_id=task.task_id,
            image_id=task.input_image.image_id,
            node_id=self._config.node_id,
            state=TaskState.FAILED,
            attempt=task.attempt,
            output_path=None,
            output_format=task.output_format,
            width=0,
            height=0,
            size_bytes=0,
            error_code=error_code,
            error_message=error_message,
            metadata=self._trace_metadata(task),
            started_at=None,
            finished_at=datetime.now(tz=UTC),
        )
        self._results[task.task_id] = result
        self._completed_at[task.task_id] = time.monotonic()
        self._metrics.failure_total.inc()
        self._state_store.append_result(result, task.idempotency_key)
        await self._emit_progress(
            ProgressEventRecord(
                task_id=task.task_id,
                image_id=task.input_image.image_id,
                node_id=self._config.node_id,
                state=TaskState.FAILED,
                progress_pct=100,
                attempt=task.attempt,
                queue_wait_ms=0,
                run_time_ms=0,
                message=error_message,
                metadata=self._trace_metadata(task),
            )
        )
        await self._reporter.report_result(result)
        self._resolve_result_waiters(task.task_id, result)

    def _cleanup_completed_cache(self) -> None:
        now = time.monotonic()
        expired = [
            task_id
            for task_id, completed_at in self._completed_at.items()
            if now - completed_at > self._config.dedupe_ttl_seconds
        ]
        for task_id in expired:
            task = self._tasks.pop(task_id, None)
            self._results.pop(task_id, None)
            self._completed_at.pop(task_id, None)
            if task is not None:
                self._idempotency_index.pop(task.idempotency_key, None)

    async def _health_loop(self) -> None:
        while not self._stop_event.is_set():
            await self.get_node_state()
            await asyncio.sleep(1.0)

    def _update_health_from_state(self, state: NodeState) -> None:
        live = True
        ready = (
            state.mode == NodeMode.ACTIVE
            and state.accepting_tasks
            and state.queue_length < self._config.queue_high_watermark
            and state.available_memory_bytes > self._config.min_free_memory_bytes
            and self._reporter.connected
        )
        if ready:
            health_state = HealthState.READY
            message = "worker ready"
        elif live and self._reporter.connected:
            health_state = HealthState.DEGRADED
            message = "worker live but not ready for new load"
        else:
            health_state = HealthState.NOT_READY
            message = "worker cannot safely accept new load"
        self._latest_health = NodeHealth(
            node_id=state.node_id,
            state=health_state,
            live=live,
            ready=ready,
            coordinator_connected=self._reporter.connected,
            queue_length=state.queue_length,
            active_tasks=state.active_tasks,
            message=message,
        )
        self._metrics.set_readiness(ready)

    def _restore_completed_state(self) -> None:
        persisted = self._state_store.load_completed()
        now = datetime.now(tz=UTC)
        for idempotency_key, record in persisted.items():
            age_seconds = (now - record.finished_at).total_seconds()
            if age_seconds > self._config.dedupe_ttl_seconds:
                continue
            result = ExecutionResultRecord(
                task_id=record.task_id,
                image_id=record.image_id,
                node_id=self._config.node_id,
                state=record.state,
                attempt=0,
                output_path=record.output_path,
                output_format=record.output_format,
                width=0,
                height=0,
                size_bytes=0,
                error_code=record.error_code,
                error_message=record.error_message,
                metadata={},
                started_at=None,
                finished_at=record.finished_at,
            )
            self._idempotency_index[idempotency_key] = record.task_id
            self._results[record.task_id] = result
            self._completed_at[record.task_id] = time.monotonic() - max(age_seconds, 0.0)

    async def _restore_pending_tasks(self) -> None:
        for task in self._pending_store.load_all():
            if task.idempotency_key in self._idempotency_index:
                self._pending_store.remove(task.task_id)
                continue
            task.metadata["node_id"] = self._config.node_id
            task.enqueued_at_monotonic = time.monotonic()
            self._tasks[task.task_id] = task
            self._idempotency_index[task.idempotency_key] = task.task_id
            await self._queue.enqueue(task)

    def _submit_reply(
        self,
        accepted: bool,
        duplicate: bool,
        task_id: str,
        state: TaskState,
        queue_position: int,
        reason: str,
    ) -> dict:
        return {
            "accepted": accepted,
            "duplicate": duplicate,
            "task_id": task_id,
            "status": PROTO_STATUS[state],
            "queue_position": queue_position,
            "reason": reason,
        }

    def _build_state_root_uri(self) -> str:
        if self._config.state_uri_prefix:
            return self._storage.join(self._config.state_uri_prefix, self._config.node_id)
        return str(self._config.state_dir)

    def _validate_storage_requirements(self) -> None:
        if not self._config.require_shared_storage:
            return
        if not self._config.state_uri_prefix:
            raise RuntimeError("shared storage is required: WORKER_STATE_URI_PREFIX must be configured")
        if not self._config.output_uri_prefix:
            raise RuntimeError("shared storage is required: WORKER_OUTPUT_URI_PREFIX must be configured")
        if "://" not in self._config.state_uri_prefix or self._config.state_uri_prefix.startswith("file://"):
            raise RuntimeError("shared storage is required: WORKER_STATE_URI_PREFIX must point to a remote URI")
        if "://" not in self._config.output_uri_prefix or self._config.output_uri_prefix.startswith("file://"):
            raise RuntimeError("shared storage is required: WORKER_OUTPUT_URI_PREFIX must point to a remote URI")

    def _trace_metadata(self, task: Task) -> dict[str, str]:
        metadata: dict[str, str] = {}
        copy_internal_trace_metadata(task.metadata, metadata)
        return metadata

    def _resolve_result_waiters(self, task_id: str, result: ExecutionResultRecord) -> None:
        for waiter in self._result_waiters.pop(task_id, []):
            if not waiter.done():
                waiter.set_result(result)
