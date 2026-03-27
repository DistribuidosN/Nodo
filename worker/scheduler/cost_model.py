from __future__ import annotations

from collections import defaultdict

from worker.models.types import OperationType, Task


OPERATION_COST_WEIGHTS: dict[OperationType, float] = {
    OperationType.GRAYSCALE: 0.6,
    OperationType.RESIZE: 0.8,
    OperationType.CROP: 0.4,
    OperationType.ROTATE: 0.7,
    OperationType.FLIP: 0.3,
    OperationType.BLUR: 1.2,
    OperationType.SHARPEN: 1.1,
    OperationType.BRIGHTNESS_CONTRAST: 0.7,
    OperationType.WATERMARK_TEXT: 0.9,
    OperationType.FORMAT_CONVERSION: 0.5,
    OperationType.OCR: 2.5,
    OperationType.INFERENCE: 3.0,
}


class ServiceTimeEstimator:
    def __init__(self, alpha: float = 0.35) -> None:
        self._alpha = alpha
        self._ema_ms: dict[str, float] = defaultdict(float)

    def estimate(self, task: Task) -> float:
        signature = self.signature(task)
        heuristic = self._heuristic_ms(task)
        learned = self._ema_ms.get(signature, 0.0)
        estimate = learned if learned > 0 else heuristic
        task.estimated_service_ms = estimate
        task.estimated_cost = estimate / 1000.0
        return estimate

    def update(self, task: Task, observed_ms: float) -> None:
        signature = self.signature(task)
        previous = self._ema_ms.get(signature, 0.0)
        if previous <= 0:
            self._ema_ms[signature] = observed_ms
            return
        self._ema_ms[signature] = self._alpha * observed_ms + (1.0 - self._alpha) * previous

    def signature(self, task: Task) -> str:
        ops = "-".join(transform.operation.value for transform in task.transforms)
        megapixel_bucket = int(max(task.input_image.width * task.input_image.height, 1) / 500_000)
        size_bucket = int(max(task.input_image.size_bytes, 1) / (1024 * 1024))
        return f"{ops}:{megapixel_bucket}:{size_bucket}"

    def _heuristic_ms(self, task: Task) -> float:
        width = max(task.input_image.width, 1)
        height = max(task.input_image.height, 1)
        megapixels = max((width * height) / 1_000_000.0, task.input_image.size_bytes / (1024 * 1024 * 6), 0.2)
        transform_weight = sum(OPERATION_COST_WEIGHTS.get(item.operation, 1.0) for item in task.transforms) or 0.5
        return 40.0 + (megapixels * 70.0 * transform_weight)
