from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from worker.domain.models import Task, ExecutionResultRecord, ProgressEventRecord, NodeState

class TaskProviderPort(ABC):
    """Interfaz para obtener nuevas tareas del exterior."""
    @abstractmethod
    async def pull_tasks(self, slots_free: int, state: NodeState) -> list[Task]:
        """Pide un número determinado de tareas."""
        pass

class ResultReporterPort(ABC):
    """Interfaz para informar el estado y resultado de las tareas."""
    @abstractmethod
    async def report_progress(self, event: ProgressEventRecord) -> None:
        """Informa el progreso parcial de una tarea."""
        pass

    @abstractmethod
    async def report_result(self, result: ExecutionResultRecord) -> None:
        """Informa el resultado final (éxito o fallo) de una tarea."""
        pass

class HeartbeatPort(ABC):
    """Interfaz para informar la salud y métricas del nodo."""
    @abstractmethod
    async def send_heartbeat(self, state: NodeState) -> None:
        """Envía métricas de salud al orquestador."""
        pass
