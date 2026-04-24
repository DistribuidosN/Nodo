from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from worker.domain.models import Task, OperationType

class ImageProcessorPort(ABC):
    """
    Puerto para el motor de procesamiento de imágenes.
    Cualquier adaptador (Pillow, OpenCV, etc.) debe implementar esta interfaz.
    """

    @abstractmethod
    def process_task(self, task: Task, output_dir: str) -> tuple[str, str, int, int, int, dict[str, str]]:
        """
        Procesa una tarea completa de imagen.
        Retorna: (output_path, format, width, height, size_bytes, metadata)
        """
        pass

    @abstractmethod
    def process_transform_stage(
        self,
        task: Task,
        payload: bytes,
        operation: OperationType,
        params: dict[str, str],
        stage_output_format: str = "png"
    ) -> tuple[bytes, str, int, int, dict[str, str]]:
        """
        Procesa una etapa individual de transformación.
        Retorna: (output_bytes, format, width, height, metadata)
        """
        pass
