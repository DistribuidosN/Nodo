from __future__ import annotations

from collections import defaultdict
from math import isclose

from worker.models.types import OperationType, Task, TransformationSpec


BASE_OPERATION_COST_WEIGHTS: dict[OperationType, float] = {
    OperationType.GRAYSCALE: 0.35,
    OperationType.RESIZE: 0.95,
    OperationType.CROP: 0.25,
    OperationType.ROTATE: 0.65,
    OperationType.FLIP: 0.20,
    OperationType.BLUR: 1.30,
    OperationType.SHARPEN: 1.00,
    OperationType.BRIGHTNESS_CONTRAST: 0.60,
    OperationType.WATERMARK_TEXT: 1.10,
    OperationType.FORMAT_CONVERSION: 0.30,
    OperationType.OCR: 3.20,
    OperationType.INFERENCE: 3.80,
}

FILTER_QUEUE_COST_HINTS: dict[str, float] = {
    "grayscale": BASE_OPERATION_COST_WEIGHTS[OperationType.GRAYSCALE],
    "resize": BASE_OPERATION_COST_WEIGHTS[OperationType.RESIZE],
    "crop": BASE_OPERATION_COST_WEIGHTS[OperationType.CROP],
    "rotate": BASE_OPERATION_COST_WEIGHTS[OperationType.ROTATE],
    "flip": BASE_OPERATION_COST_WEIGHTS[OperationType.FLIP],
    "blur": BASE_OPERATION_COST_WEIGHTS[OperationType.BLUR],
    "sharpen": BASE_OPERATION_COST_WEIGHTS[OperationType.SHARPEN],
    "brightness": BASE_OPERATION_COST_WEIGHTS[OperationType.BRIGHTNESS_CONTRAST],
    "contrast": BASE_OPERATION_COST_WEIGHTS[OperationType.BRIGHTNESS_CONTRAST],
    "brightness_contrast": BASE_OPERATION_COST_WEIGHTS[OperationType.BRIGHTNESS_CONTRAST],
    "watermark": BASE_OPERATION_COST_WEIGHTS[OperationType.WATERMARK_TEXT],
    "watermark_text": BASE_OPERATION_COST_WEIGHTS[OperationType.WATERMARK_TEXT],
    "format": BASE_OPERATION_COST_WEIGHTS[OperationType.FORMAT_CONVERSION],
    "format_conversion": BASE_OPERATION_COST_WEIGHTS[OperationType.FORMAT_CONVERSION],
    "ocr": BASE_OPERATION_COST_WEIGHTS[OperationType.OCR],
    "inference": BASE_OPERATION_COST_WEIGHTS[OperationType.INFERENCE],
}

FORMAT_CONVERSION_COST_MULTIPLIERS: dict[str, float] = {
    "jpg": 1.0,
    "jpeg": 1.0,
    "png": 1.05,
    "webp": 1.25,
    "bmp": 0.85,
    "gif": 1.35,
    "ico": 1.20,
    "tif": 1.15,
    "tiff": 1.15,
}

FORMAT_CONVERSION_QUEUE_COST_HINTS: dict[str, float] = {
    image_format: round(BASE_OPERATION_COST_WEIGHTS[OperationType.FORMAT_CONVERSION] * multiplier, 3)
    for image_format, multiplier in FORMAT_CONVERSION_COST_MULTIPLIERS.items()
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
        transform_weight = sum(estimate_transform_weight(task, item) for item in task.transforms) or 0.5
        return 40.0 + (megapixels * 70.0 * transform_weight)


def estimate_transform_weight(task: Task, transform: TransformationSpec) -> float:
    base = BASE_OPERATION_COST_WEIGHTS.get(transform.operation, 1.0)
    params = transform.params

    if transform.operation == OperationType.RESIZE:
        source_area = max(task.input_image.width * task.input_image.height, 1)
        target_width = max(int(params.get("width", str(task.input_image.width or 1))), 1)
        target_height = max(int(params.get("height", str(task.input_image.height or 1))), 1)
        target_area = target_width * target_height
        area_ratio = target_area / source_area
        if area_ratio >= 1.0:
            return base * min(1.0 + ((area_ratio - 1.0) * 0.35), 2.2)
        return base * max(0.65, 0.9 - ((1.0 - area_ratio) * 0.25))

    if transform.operation == OperationType.CROP:
        source_area = max(task.input_image.width * task.input_image.height, 1)
        left = float(params.get("left", "0"))
        upper = float(params.get("upper", "0"))
        right = float(params.get("right", str(task.input_image.width or 1)))
        lower = float(params.get("lower", str(task.input_image.height or 1)))
        crop_width = max(right - left, 1.0)
        crop_height = max(lower - upper, 1.0)
        crop_area = crop_width * crop_height
        crop_ratio = crop_area / source_area
        return base * max(0.45, min(1.0, crop_ratio + 0.15))

    if transform.operation == OperationType.ROTATE:
        angle = abs(float(params.get("angle", "0"))) % 360
        expand = params.get("expand", "true").lower() == "true"
        if isclose(angle, 0.0, abs_tol=1e-9):
            return 0.05
        if angle in {90.0, 180.0, 270.0}:
            quadrant_multiplier = 0.70 if angle == 180.0 else 0.85
            return base * (quadrant_multiplier if expand else quadrant_multiplier + 0.10)
        return base * 1.30

    if transform.operation == OperationType.FLIP:
        direction = params.get("direction", "horizontal").lower()
        return base * (1.15 if direction == "both" else 1.0)

    if transform.operation == OperationType.BLUR:
        radius = max(float(params.get("radius", "1.5")), 0.0)
        return base * min(0.85 + (radius * 0.22), 2.0)

    if transform.operation == OperationType.SHARPEN:
        factor = max(float(params.get("factor", "2.0")), 0.0)
        deviation = abs(factor - 1.0)
        return base * min(0.9 + (deviation * 0.35), 1.9)

    if transform.operation == OperationType.BRIGHTNESS_CONTRAST:
        brightness = max(float(params.get("brightness", "1.0")), 0.0)
        contrast = max(float(params.get("contrast", "1.0")), 0.0)
        deviation = max(abs(brightness - 1.0), abs(contrast - 1.0))
        return base * min(0.9 + (deviation * 0.4), 1.8)

    if transform.operation == OperationType.WATERMARK_TEXT:
        text = params.get("text", "")
        size = max(int(params.get("size", "36")), 1)
        return base * min(0.9 + (len(text) / 80.0) + (size / 180.0), 1.9)

    if transform.operation == OperationType.FORMAT_CONVERSION:
        output_format = (task.output_format or task.input_image.image_format or "png").lower()
        return base * FORMAT_CONVERSION_COST_MULTIPLIERS.get(output_format, 1.1)

    return base
