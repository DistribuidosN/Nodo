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

# Gestión de Ejecución de Tareas
# Esta clase coordina el ciclo de vida de una tarea: desde que sale de la cola
# hasta que se procesa (ya sea en un hilo o en un proceso dedicado).

def _process_entrypoint(
    task: Task,
    payload: bytes,
    operation_name: str,
    params: dict[str, str],
    stage_output_format: str,
    result_queue: multiprocessing.Queue[ChildResult],
) -> None:
    """Punto de entrada para procesos hijos (multiprocessing)."""
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
    """Representa una tarea que está actualmente en ejecución."""
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
    """
    Controlador central de la ejecución paralela.
    Decide si una tarea corre en un Thread o en un Proceso (Isolation).
    """
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
        self._active: dict[str, RunningTask] = {}
        self._semaphore = asyncio.Semaphore(config.max_active_tasks)
        self._process_semaphore = asyncio.Semaphore(max(1, config.process_pool_workers))
        self._lock = asyncio.Lock()
        self._logger = logging.getLogger("worker.execution")
        self._cancel_dir = config.state_dir / "cancel_tokens"
        self._cancel_dir.mkdir(parents=True, exist_ok=True)
        self._mp_context = multiprocessing.get_context("spawn")

    async def start_task(self, task: Task, requirements: ResourceRequirements) -> None:
        """Admite una tarea para ejecución inmediata."""
        await self._semaphore.acquire()
        task.attempt += 1
        
        # Inyección de comandos para backends externos si están configurados
        if self._config.ocr_command and "ocr_command" not in task.metadata:
            task.metadata["ocr_command"] = self._config.ocr_command
        if self._config.inference_command and "inference_command" not in task.metadata:
            task.metadata["inference_command"] = self._config.inference_command
            
        queue_wait_ms = int((time.monotonic() - task.enqueued_at_monotonic) * 1000)
        task.status = TaskState.RUNNING
        
        # Determinamos el tipo de executor (Thread vs Process)
        executor_kind = self._pick_executor(task)
        if executor_kind is ExecutorKind.DEDICATED_PROCESS:
            await self._process_semaphore.acquire()
            
        task.metadata["executor_kind"] = executor_kind.value
        
        # Token de cancelación local
        task.cancel_token_path = str(self._cancel_dir / f"{task.task_id}.cancel")
        token_path = Path(task.cancel_token_path)
        if token_path.exists():
            token_path.unlink()

        # Reportamos que la tarea ha sido admitida
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

        try:
            running = self._start_running_task(task, requirements, queue_wait_ms, executor_kind)
            async with self._lock:
                self._active[task.task_id] = running
                self._metrics.active_tasks.set(len(self._active))

            print(f"[DEBUG] === Iniciando PROCESAMIENTO: Tarea={task.task_id}, Executor={executor_kind.value}")
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
            # El monitor se encarga de liberar recursos al terminar (éxito o fallo)
            running.watch_task = asyncio.create_task(self._watch_task(running))
            
        except Exception as exc:
            # REGLA 2 y 3: Si fallamos ANTES de iniciar el monitor, debemos liberar manual
            self._logger.exception("failed to initiate task execution", extra={"task_id": task.task_id})
            # Liberamos recursos
            if executor_kind is ExecutorKind.DEDICATED_PROCESS:
                self._process_semaphore.release()
            self._semaphore.release()
            # Informamos el fallo catastrófico
            await self._handle_failure(task, exc.__class__.__name__, f"execution startup failed: {exc}")

    async def cancel(self, task_id: str) -> bool:
        """Intenta cancelar una tarea en ejecución."""
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
        """Detiene el manager esperando a que terminen las tareas activas."""
        try:
            async with asyncio.timeout(5.0):
                await self.wait_for_idle()
        except TimeoutError:
            pass

    @property
    def active_count(self) -> int:
        return len(self._active)

    async def wait_for_idle(self) -> None:
        """Espera hasta que no haya tareas activas."""
        while True:
            async with self._lock:
                active = [item.future for item in self._active.values()]
            if not active:
                return
            await asyncio.gather(*active, return_exceptions=True)

    def _pick_executor(self, task: Task) -> ExecutorKind:
        """Decide qué executor usar. Ahora siempre usamos procesos para evitar el GIL."""
        return ExecutorKind.DEDICATED_PROCESS

    def _start_running_task(
        self,
        task: Task,
        requirements: ResourceRequirements,
        queue_wait_ms: int,
        executor_kind: ExecutorKind,
    ) -> RunningTask:
        """Arranca la ejecución física de la tarea."""
        # Se fuerza el uso de pipeline en procesos dedicados (Cero hilos para el GIL)
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

    async def _run_dedicated_pipeline(self, task: Task) -> tuple[str, str, int, int, int, dict[str, str]]:
        """Ejecuta un pipeline de imágenes en procesos hijos etapa por etapa."""
        payload, current_format = load_input_bytes(task, self._storage)
        width, height = task.input_image.width, task.input_image.height
        metadata: dict[str, str] = {"processor": "pillow", "pipeline_mode": "stage_process"}

        if not task.transforms:
            output_format = (task.output_format or current_format or "png").lower()
            output_path, size_bytes = persist_output_bytes(
                task.task_id, payload, output_format, str(self._config.output_dir)
            )
            return str(output_path), output_format, width, height, size_bytes, metadata

        for index, transform in enumerate(task.transforms):
            self._check_cancel_token(task)
            stage_format = (task.output_format or current_format or "png").lower() if index == len(task.transforms) - 1 else "png"

            result_queue: multiprocessing.Queue[ChildResult] = self._mp_context.Queue(maxsize=1)
            process: BaseProcess = self._mp_context.Process(
                target=_process_entrypoint,
                args=(task, payload, transform.operation.value, dict(transform.params), stage_format, result_queue),
                name=f"worker-stage-{index}"
            )
            process.start()

            async with self._lock:
                running = self._active.get(task.task_id)
                if running:
                    running.process, running.process_queue = process, result_queue
            
            payload, current_format, width, height, stage_metadata = await self._await_process_result(process, result_queue)
            metadata.update(stage_metadata)

        output_path, size_bytes = persist_output_bytes(task.task_id, payload, current_format, str(self._config.output_dir))
        return str(output_path), current_format, width, height, size_bytes, metadata

    async def _await_process_result(self, process: BaseProcess, result_queue: multiprocessing.Queue[ChildResult]) -> any:
        try:
            while True:
                item = _poll_child_result(process, result_queue)
                if item is None:
                    await asyncio.sleep(0.05)
                    continue
                return _handle_child_result(item)
        finally:
           await asyncio.to_thread(process.join, 30)
           result_queue.close()

    async def _graceful_cancel_then_kill(self, running: RunningTask) -> None:
        """Intento de terminación amable antes de matar el proceso."""
        await asyncio.sleep(self._config.process_cancel_grace_seconds)
        if running.future.done(): return
        if running.process and running.process.is_alive():
            running.process.terminate()
            await asyncio.to_thread(running.process.join, self._config.process_kill_timeout_seconds)
            if running.process.is_alive():
                kill = getattr(running.process, "kill", None)
                if callable(kill): kill()

    def _check_cancel_token(self, task: Task) -> None:
        if task.cancel_token_path and Path(task.cancel_token_path).exists():
            raise TaskCancelledError(f"task {task.task_id} cancelled")

    async def _watch_task(self, running: RunningTask) -> None:
        """Observa el futuro de la tarea y maneja el resultado final."""
        task = running.task
        try:
            # Observación: ya no se usa tracing/spans aquí para simplicidad
            output_path, output_format, width, height, size_bytes, metadata = await running.future
            
            run_time_ms = int((time.monotonic() - running.started_at_monotonic) * 1000)
            print(f"[DEBUG] === PROCESAMIENTO completado: Tarea={task.task_id}, Tiempo={run_time_ms}ms, Path={output_path}")
            self._estimator.update(task, run_time_ms)
            self._metrics.record_processing_time_ms(run_time_ms)
            
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
            
        except (asyncio.CancelledError, TaskCancelledError):
            run_time_ms = int((time.monotonic() - running.started_at_monotonic) * 1000)
            self._cleanup_partial_outputs(task)
            await self._handle_result(task, self._cancelled_result(task, running), running.queue_wait_ms, run_time_ms)
        except Exception as exc:
            self._logger.exception("task execution failed", extra={"task_id": task.task_id})
            await self._handle_failure(task, exc.__class__.__name__, str(exc))
        finally:
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
            output_format=task.output_format,
            error_code="cancelled",
            error_message="task cancelled",
            metadata=self._trace_metadata(task),
            started_at=running.started_at,
            finished_at=datetime.now(tz=UTC),
        )

    def _trace_metadata(self, task: Task) -> dict[str, str]:
        """Metadatos de trazado (vaciados tras remover tracing)."""
        return {}


def _poll_child_result(process: BaseProcess, result_queue: multiprocessing.Queue[ChildResult]) -> ChildResult | None:
    try:
        return result_queue.get_nowait()
    except queue.Empty:
        if not process.is_alive():
            raise TaskCancelledError(f"dedicated process exited with code {process.exitcode}")
        return None

def _handle_child_result(item: ChildResult) -> any:
    status = item[0]
    if status == "ok": return item[1]
    if status == "cancelled": raise TaskCancelledError(item[1])
    raise RuntimeError(f"{item[1]}: {item[2]}")

ChildOk = tuple[Literal["ok"], tuple[bytes, str, int, int, dict[str, str]]]
ChildCancelled = tuple[Literal["cancelled"], str]
ChildError = tuple[Literal["error"], str, str]
ChildResult = Union[ChildOk, ChildCancelled, ChildError]
