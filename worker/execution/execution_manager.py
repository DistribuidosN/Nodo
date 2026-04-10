from __future__ import annotations

import asyncio
import logging
import multiprocessing
import queue
import time
from concurrent.futures import Executor, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Awaitable, Callable, Literal, Union, cast
from multiprocessing.process import BaseProcess

from worker.config import WorkerConfig
from worker.core.storage import StorageClient
from worker.execution.image_processor import (
    TaskCancelledError,
    load_input_bytes,
    persist_output_bytes,
    process_image,
    process_transform_stage,
)
from worker.models.types import (
    ExecutionResultRecord,
    ExecutorKind,
    ProgressEventRecord,
    ResourceRequirements,
    Task,
    TaskState,
)
from worker.scheduler.cost_model import ServiceTimeEstimator
from worker.telemetry.metrics import WorkerMetrics
from worker.telemetry.tracing import copy_internal_trace_metadata, extract_context_from_internal_metadata, maybe_span


def _process_entrypoint(
    task: Task,
    payload: bytes,
    operation_name: str,
    params: dict[str, str],
    stage_output_format: str,
    result_queue: multiprocessing.Queue[ChildResult],
) -> None:
    try:
        result = process_transform_stage(
            task=task,
            payload=payload,
            operation=task.transforms[0].operation.__class__(operation_name),
            params=params,
            stage_output_format=stage_output_format,
        )
        result_queue.put(("ok", result))
    except TaskCancelledError as exc:
        result_queue.put(("cancelled", str(exc)))
    except Exception as exc:  # pragma: no cover
        result_queue.put(("error", exc.__class__.__name__, str(exc)))


@dataclass(slots=True)
class RunningTask:
    task: Task
    requirements: ResourceRequirements
    future: asyncio.Future[tuple[str, str, int, int, int, dict[str, str]]]
    started_at: datetime
    started_at_monotonic: float
    queue_wait_ms: int
    cancel_token_path: str | None
    executor_kind: ExecutorKind
    process: BaseProcess | None = None
    process_queue: multiprocessing.Queue[ChildResult] | None = None
    cancel_task: asyncio.Task[None] | None = None
    watch_task: asyncio.Task[None] | None = None
    cancel_requested: bool = False


class ExecutionManager:
    def __init__(
        self,
        config: WorkerConfig,
        metrics: WorkerMetrics,
        estimator: ServiceTimeEstimator,
        emit_progress: Callable[[ProgressEventRecord], Awaitable[None]],
        handle_result: Callable[[Task, ExecutionResultRecord, int, int], Awaitable[None]],
        handle_failure: Callable[[Task, str, str], Awaitable[None]],
        release_resources: Callable[[str], Awaitable[None]],
        storage: StorageClient | None = None,
    ) -> None:
        self._config = config
        self._metrics = metrics
        self._estimator = estimator
        self._emit_progress = emit_progress
        self._handle_result = handle_result
        self._handle_failure = handle_failure
        self._release_resources = release_resources
        self._storage = storage or StorageClient()
        self._thread_pool = ThreadPoolExecutor(max_workers=config.thread_pool_workers)
        self._active: dict[str, RunningTask] = {}
        self._semaphore = asyncio.Semaphore(config.max_active_tasks)
        self._process_semaphore = asyncio.Semaphore(max(1, config.process_pool_workers))
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("worker.execution")
        self._cancel_dir = config.state_dir / "cancel_tokens"
        self._cancel_dir.mkdir(parents=True, exist_ok=True)
        self._mp_context = multiprocessing.get_context("spawn")

    async def start_task(self, task: Task, requirements: ResourceRequirements) -> None:
        await self._semaphore.acquire()
        task.attempt += 1
        if self._config.ocr_command and "ocr_command" not in task.metadata:
            task.metadata["ocr_command"] = self._config.ocr_command
        if self._config.inference_command and "inference_command" not in task.metadata:
            task.metadata["inference_command"] = self._config.inference_command
        task.metadata.setdefault("adapter_timeout_seconds", str(self._config.adapter_timeout_seconds))
        queue_wait_ms = int((time.monotonic() - task.enqueued_at_monotonic) * 1000)
        task.status = TaskState.RUNNING
        executor_kind = self._pick_executor(task)
        if executor_kind is ExecutorKind.DEDICATED_PROCESS:
            await self._process_semaphore.acquire()
        task.metadata["executor_kind"] = executor_kind.value
        task.cancel_token_path = str(self._cancel_dir / f"{task.task_id}.cancel")
        token_path = Path(task.cancel_token_path)
        if token_path.exists():
            token_path.unlink()

        await self._emit_progress(
            ProgressEventRecord(
                task_id=task.task_id,
                image_id=task.input_image.image_id,
                node_id=task.metadata["node_id"],
                state=TaskState.ADMITTED,
                progress_pct=5,
                attempt=task.attempt,
                queue_wait_ms=queue_wait_ms,
                run_time_ms=0,
                message="task admitted by local scheduler",
                metadata=self._trace_metadata(task),
            )
        )

        running = self._start_running_task(task, requirements, queue_wait_ms, executor_kind)
        async with self._lock:
            self._active[task.task_id] = running
            self._metrics.active_tasks.set(len(self._active))

        await self._emit_progress(
            ProgressEventRecord(
                task_id=task.task_id,
                image_id=task.input_image.image_id,
                node_id=task.metadata["node_id"],
                state=TaskState.RUNNING,
                progress_pct=15,
                attempt=task.attempt,
                queue_wait_ms=queue_wait_ms,
                run_time_ms=0,
                message=f"task running in {executor_kind.value}",
                metadata=self._trace_metadata(task),
            )
        )
        running.watch_task = asyncio.create_task(self._watch_task(running))

    async def cancel(self, task_id: str) -> bool:
        async with self._lock:
            running = self._active.get(task_id)
            if running is None:
                return False
            running.cancel_requested = True

        if running.cancel_token_path is not None:
            Path(running.cancel_token_path).write_text("cancel", encoding="utf-8")
        if running.executor_kind is ExecutorKind.DEDICATED_PROCESS and running.process is not None:
            if running.cancel_task is None or running.cancel_task.done():
                running.cancel_task = asyncio.create_task(self._graceful_cancel_then_kill(running))
            return True
        running.future.cancel()
        return True

    async def shutdown(self) -> None:
        try:
            async with asyncio.timeout(5.0):
                await self.wait_for_idle()
        except TimeoutError:
            pass
        self._thread_pool.shutdown(wait=True, cancel_futures=True)

    @property
    def active_count(self) -> int:
        return len(self._active)

    async def wait_for_idle(self) -> None:
        async def _await_all() -> None:
            while True:
                async with self._lock:
                    active = [item.future for item in self._active.values()]
                if not active:
                    return
                await asyncio.gather(*active, return_exceptions=True)

        await _await_all()

    def _pick_executor(self, task: Task) -> ExecutorKind:
        if task.metadata.get("execution_isolation") == "process":
            return ExecutorKind.DEDICATED_PROCESS
        non_cooperative_ops = {"ocr", "inference"}
        if any(op.operation.value in non_cooperative_ops for op in task.transforms):
            return ExecutorKind.DEDICATED_PROCESS
        return ExecutorKind.THREAD

    def _executor_for(self) -> Executor:
        return self._thread_pool

    def _start_running_task(
        self,
        task: Task,
        requirements: ResourceRequirements,
        queue_wait_ms: int,
        executor_kind: ExecutorKind,
    ) -> RunningTask:
        loop = asyncio.get_running_loop()
        if executor_kind is ExecutorKind.DEDICATED_PROCESS:
            future = asyncio.create_task(self._run_dedicated_pipeline(task))
            return RunningTask(
                task=task,
                requirements=requirements,
                future=future,
                started_at=datetime.now(tz=UTC),
                started_at_monotonic=time.monotonic(),
                queue_wait_ms=queue_wait_ms,
                cancel_token_path=task.cancel_token_path,
                executor_kind=executor_kind,
            )

        future = loop.run_in_executor(
            self._executor_for(),
            process_image,
            task,
            str(self._config.output_dir),
            self._storage,
        )
        return RunningTask(
            task=task,
            requirements=requirements,
            future=future,
            started_at=datetime.now(tz=UTC),
            started_at_monotonic=time.monotonic(),
            queue_wait_ms=queue_wait_ms,
            cancel_token_path=task.cancel_token_path,
            executor_kind=executor_kind,
        )

    async def _await_process_result(
        self,
        process: BaseProcess,
        result_queue: multiprocessing.Queue[ChildResult],
    ) -> tuple[bytes, str, int, int, dict[str, str]]:
        try:
            while True:
                item = _poll_child_result(process, result_queue)
                if item is None:
                    await asyncio.sleep(0.05)
                    continue
                return _handle_child_result(item)
        finally:
            await asyncio.to_thread(process.join, self._config.process_kill_timeout_seconds)
            result_queue.close()
            result_queue.join_thread()

    async def _run_dedicated_pipeline(self, task: Task) -> tuple[str, str, int, int, int, dict[str, str]]:
        payload, current_format = load_input_bytes(task, self._storage)
        width = task.input_image.width
        height = task.input_image.height
        metadata: dict[str, str] = {"processor": "pillow", "pipeline_mode": "stage_process"}

        if not task.transforms:
            output_format = (task.output_format or current_format or "png").lower()
            output_path, size_bytes = persist_output_bytes(
                task.task_id,
                payload,
                output_format,
                str(self._config.output_dir),
            )
            return str(output_path), output_format, width, height, size_bytes, metadata

        for index, transform in enumerate(task.transforms):
            self._check_cancel_token(task)
            stage_output_format = "png"
            is_last = index == len(task.transforms) - 1
            if is_last:
                stage_output_format = (task.output_format or current_format or "png").lower()

            result_queue: multiprocessing.Queue[ChildResult] = self._mp_context.Queue(maxsize=1)
            process: BaseProcess = self._mp_context.Process(
                target=_process_entrypoint,
                args=(
                    task,
                    payload,
                    transform.operation.value,
                    dict(transform.params),
                    stage_output_format,
                    result_queue,
                ),
                name=f"worker-task-{task.task_id}-stage-{index}",
            )
            process.start()

            async with self._lock:
                running = self._active.get(task.task_id)
                if running is not None:
                    running.process = process
                    running.process_queue = result_queue
            task.metadata["child_pid"] = str(process.pid)
            payload, current_format, width, height, stage_metadata = await self._await_process_result(process, result_queue)
            metadata.update(stage_metadata)
            self._check_cancel_token(task)

        output_path, size_bytes = persist_output_bytes(
            task.task_id,
            payload,
            current_format,
            str(self._config.output_dir),
        )
        return str(output_path), current_format, width, height, size_bytes, metadata

    async def _terminate_process(self, process: BaseProcess) -> None:
        if not process.is_alive():
            return
        process.terminate()
        await asyncio.to_thread(process.join, self._config.process_kill_timeout_seconds)
        if process.is_alive():
            kill = getattr(process, "kill", None)
            if callable(kill):
                kill()
                await asyncio.to_thread(process.join, self._config.process_kill_timeout_seconds)

    async def _graceful_cancel_then_kill(self, running: RunningTask) -> None:
        await asyncio.sleep(self._config.process_cancel_grace_seconds)
        if running.future.done():
            return
        process = running.process
        if process is not None and process.is_alive():
            await self._terminate_process(process)

    def _check_cancel_token(self, task: Task) -> None:
        if task.cancel_token_path and Path(task.cancel_token_path).exists():
            raise TaskCancelledError(f"task {task.task_id} cancelled")

    async def _watch_task(self, running: RunningTask) -> None:
        task = running.task
        try:
            span_context = extract_context_from_internal_metadata(task.metadata)
            with maybe_span(
                "worker.task.execute",
                context=span_context,
                attributes={
                    "worker.task.id": task.task_id,
                    "worker.image.id": task.input_image.image_id,
                    "worker.executor.kind": running.executor_kind.value,
                },
            ):
                output_path, output_format, width, height, size_bytes, metadata = await running.future
                run_time_ms = int((time.monotonic() - running.started_at_monotonic) * 1000)
                self._estimator.update(task, run_time_ms)
                self._metrics.processing_time_ms.observe(run_time_ms)
                result = ExecutionResultRecord(
                    task_id=task.task_id,
                    image_id=task.input_image.image_id,
                    node_id=task.metadata["node_id"],
                    state=TaskState.SUCCEEDED,
                    attempt=task.attempt,
                    output_path=output_path,
                    output_format=output_format,
                    width=width,
                    height=height,
                    size_bytes=size_bytes,
                    metadata={**metadata, **self._trace_metadata(task)},
                    started_at=running.started_at,
                    finished_at=datetime.now(tz=UTC),
                )
                await self._handle_result(task, result, running.queue_wait_ms, run_time_ms)
        except asyncio.CancelledError:
            await self._handle_result(task, self._cancelled_result(task, running), running.queue_wait_ms, 0)
            raise
        except TaskCancelledError:
            run_time_ms = int((time.monotonic() - running.started_at_monotonic) * 1000)
            self._cleanup_partial_outputs(task)
            await self._handle_result(task, self._cancelled_result(task, running), running.queue_wait_ms, run_time_ms)
        except Exception as exc:  # pragma: no cover
            self._logger.exception("task execution failed", extra={"task_id": task.task_id})
            await self._handle_failure(task, exc.__class__.__name__, str(exc))
        finally:
            if running.cancel_task is not None:
                running.cancel_task.cancel()
            if running.cancel_token_path and Path(running.cancel_token_path).exists():
                Path(running.cancel_token_path).unlink(missing_ok=True)
            async with self._lock:
                self._active.pop(task.task_id, None)
                self._metrics.active_tasks.set(len(self._active))
            if running.executor_kind is ExecutorKind.DEDICATED_PROCESS:
                self._process_semaphore.release()
            await self._release_resources(task.task_id)
            self._semaphore.release()

    def _cleanup_partial_outputs(self, task: Task) -> None:
        for path in self._config.output_dir.glob(f"{task.task_id}.*"):
            path.unlink(missing_ok=True)

    def _cancelled_result(self, task: Task, running: RunningTask) -> ExecutionResultRecord:
        return ExecutionResultRecord(
            task_id=task.task_id,
            image_id=task.input_image.image_id,
            node_id=task.metadata["node_id"],
            state=TaskState.CANCELLED,
            attempt=task.attempt,
            output_path=None,
            output_format=task.output_format,
            width=0,
            height=0,
            size_bytes=0,
            error_code="cancelled",
            error_message="task cancelled",
            metadata=self._trace_metadata(task),
            started_at=running.started_at,
            finished_at=datetime.now(tz=UTC),
        )

    def _trace_metadata(self, task: Task) -> dict[str, str]:
        metadata: dict[str, str] = {}
        copy_internal_trace_metadata(task.metadata, metadata)
        return metadata


ChildOk = tuple[Literal["ok"], tuple[bytes, str, int, int, dict[str, str]]]
ChildCancelled = tuple[Literal["cancelled"], str]
ChildError = tuple[Literal["error"], str, str]
ChildResult = Union[ChildOk, ChildCancelled, ChildError]

def _poll_child_result(
    process: BaseProcess,
    result_queue: multiprocessing.Queue[ChildResult],
) -> ChildResult | None:
    try:
        return result_queue.get_nowait()
    except queue.Empty:
        if not process.is_alive():
            exitcode = process.exitcode
            if exitcode == 0:
                raise RuntimeError("dedicated process exited without returning a result")
            raise TaskCancelledError(f"dedicated process exited with code {exitcode}")
        return None


def _handle_child_result(item: ChildResult) -> tuple[bytes, str, int, int, dict[str, str]]:
    status = item[0]
    if status == "ok":
        return cast(tuple[bytes, str, int, int, dict[str, str]], item[1])
    if status == "cancelled":
        cancelled = cast(ChildCancelled, item)
        raise TaskCancelledError(cancelled[1])
    if status == "error":
        err = cast(ChildError, item)
        raise RuntimeError(f"{err[1]}: {err[2]}")
    raise RuntimeError(f"unknown child process result status: {status}")
