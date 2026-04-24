from __future__ import annotations
import asyncio
import logging
import contextlib
from typing import TYPE_CHECKING, Callable, Awaitable

if TYPE_CHECKING:
    from worker.application.ports.coordinator import TaskProviderPort, ResultReporterPort, HeartbeatPort
    from worker.domain.models import Task, NodeState, ProgressEventRecord, ExecutionResultRecord

class CommunicationService:
    """
    Servicio de la capa de aplicación que coordina la comunicación externa.
    Maneja los bucles de vida de red de forma agnóstica a la infraestructura.
    """
    def __init__(
        self,
        task_provider: TaskProviderPort,
        result_reporter: ResultReporterPort,
        heartbeat_port: HeartbeatPort,
        status_provider: Callable[[], Awaitable[NodeState]],
        submit_task: Callable[[Task], Awaitable[bool]],
        pull_interval: float = 1.0,
        heartbeat_interval: float = 5.0
    ) -> None:
        self._task_provider = task_provider
        self._result_reporter = result_reporter
        self._heartbeat_port = heartbeat_port
        self._status_provider = status_provider
        self._submit_task = submit_task
        self._pull_interval = pull_interval
        self._heartbeat_interval = heartbeat_interval
        
        self._logger = logging.getLogger("worker.communication")
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Inicia los bucles de comunicación."""
        self._logger.info("Starting communication service loops")
        self._tasks = [
            asyncio.create_task(self._pull_loop(), name="pull-loop"),
            asyncio.create_task(self._heartbeat_loop(), name="heartbeat-loop")
        ]

    async def stop(self) -> None:
        """Detiene los bucles de forma segura."""
        self._stop_event.set()
        for t in self._tasks:
            t.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*self._tasks)

    async def report_result(self, result: ExecutionResultRecord) -> None:
        """Reporta un resultado final al exterior."""
        await self._result_reporter.report_result(result)

    async def report_progress(self, event: ProgressEventRecord) -> None:
        """Reporta progreso parcial al exterior."""
        await self._result_reporter.report_progress(event)

    async def _pull_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                state = await self._status_provider()
                # Lógica de Backpressure (Regla 1 de la refactorización previa)
                slots_free = max(10 - state.queue_length, 0) # Simplificado
                
                if slots_free > 0:
                    tasks = await self._task_provider.pull_tasks(slots_free, state)
                    for t in tasks:
                        await self._submit_task(t)
            except Exception as e:
                self._logger.error(f"Error in pull loop: {e}")
            await asyncio.sleep(self._pull_interval)

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                state = await self._status_provider()
                await self._heartbeat_port.send_heartbeat(state)
            except Exception as e:
                self._logger.error(f"Error in heartbeat loop: {e}")
            await asyncio.sleep(self._heartbeat_interval)
