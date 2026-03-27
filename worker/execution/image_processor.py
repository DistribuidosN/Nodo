from __future__ import annotations

import math
import time
from io import BytesIO
from pathlib import Path

from PIL import Image, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

from worker.models.types import OperationType, Task


FORMAT_MAP = {
    "jpg": "JPEG",
    "jpeg": "JPEG",
    "png": "PNG",
    "tif": "TIFF",
    "tiff": "TIFF",
}

DEFAULT_TILE_SIZE = 256


class TaskCancelledError(Exception):
    """Raised when a running task observes a cancellation token."""


def process_image(task: Task, output_dir: str) -> tuple[str, str, int, int, int, dict[str, str]]:
    _check_cancelled(task)
    image = _load_image(task)
    for transform in task.transforms:
        image = apply_transform_checked(task, image, transform.operation, transform.params)

    requested_format = (task.output_format or task.input_image.image_format or "png").lower()
    output_bytes, save_format = serialize_image(image, requested_format)
    width, height = image.size
    output_path, size_bytes = persist_output_bytes(task.task_id, output_bytes, requested_format, output_dir)
    return str(output_path), requested_format, width, height, size_bytes, {"processor": "pillow"}


def process_transform_stage(
    task: Task,
    payload: bytes,
    input_format: str | None,
    operation: OperationType,
    params: dict[str, str],
    stage_output_format: str = "png",
) -> tuple[bytes, str, int, int, dict[str, str]]:
    _check_cancelled(task)
    image = load_image_bytes(payload)
    image = apply_transform_checked(task, image, operation, params)
    output_bytes, _save_format = serialize_image(image, stage_output_format)
    width, height = image.size
    return output_bytes, stage_output_format, width, height, {"processor": "pillow", "stage_operation": operation.value}


def load_input_bytes(task: Task) -> tuple[bytes, str]:
    if task.input_image.payload:
        return task.input_image.payload, (task.input_image.image_format or "png").lower()
    if task.input_image.input_path:
        path = Path(task.input_image.input_path)
        return path.read_bytes(), (task.input_image.image_format or path.suffix.lstrip(".") or "png").lower()
    if task.input_image.input_uri:
        raise ValueError("input_uri is not supported by the local worker implementation")
    raise ValueError("task does not contain an image source")


def load_image_bytes(payload: bytes) -> Image.Image:
    return Image.open(BytesIO(payload)).copy()


def serialize_image(image: Image.Image, requested_format: str) -> tuple[bytes, str]:
    requested_format = requested_format.lower()
    save_format = FORMAT_MAP.get(requested_format, requested_format.upper())
    if save_format == "JPEG" and image.mode not in ("RGB", "L"):
        image = image.convert("RGB")
    buffer = BytesIO()
    image.save(buffer, format=save_format)
    return buffer.getvalue(), save_format


def persist_output_bytes(task_id: str, output_bytes: bytes, requested_format: str, output_dir: str) -> tuple[Path, int]:
    output_path = Path(output_dir) / f"{task_id}.{requested_format}"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp_path.write_bytes(output_bytes)
    tmp_path.replace(output_path)
    return output_path, output_path.stat().st_size


def _load_image(task: Task) -> Image.Image:
    payload, _input_format = load_input_bytes(task)
    return load_image_bytes(payload)


def _check_cancelled(task: Task) -> None:
    if task.cancel_token_path and Path(task.cancel_token_path).exists():
        raise TaskCancelledError(f"task {task.task_id} cancelled")


def _maybe_delay(params: dict[str, str]) -> None:
    delay_ms = int(params.get("delay_ms", "0"))
    if delay_ms > 0:
        time.sleep(delay_ms / 1000.0)


def apply_transform_checked(image_task: Task, image: Image.Image, operation: OperationType, params: dict[str, str]) -> Image.Image:
    _check_cancelled(image_task)
    if operation == OperationType.RESIZE:
        return _resize_cooperative(image_task, image, params)
    if operation == OperationType.ROTATE:
        return _rotate_cooperative(image_task, image, params)
    if operation == OperationType.BLUR:
        return _blur_cooperative(image_task, image, params)
    if operation == OperationType.SHARPEN:
        return _sharpen_cooperative(image_task, image, params)
    if operation == OperationType.BRIGHTNESS_CONTRAST:
        return _brightness_contrast_cooperative(image_task, image, params)
    _maybe_delay(params)
    _check_cancelled(image_task)
    return _apply_transform(image, operation, params)


def _apply_transform(image: Image.Image, operation: OperationType, params: dict[str, str]) -> Image.Image:
    if operation == OperationType.GRAYSCALE:
        return ImageOps.grayscale(image)
    if operation == OperationType.RESIZE:
        width = int(params["width"])
        height = int(params["height"])
        return image.resize((width, height), resample=Image.Resampling.LANCZOS)
    if operation == OperationType.CROP:
        box = tuple(int(params[key]) for key in ("left", "upper", "right", "lower"))
        return image.crop(box)
    if operation == OperationType.ROTATE:
        angle = float(params.get("angle", "0"))
        expand = params.get("expand", "true").lower() == "true"
        return image.rotate(angle, expand=expand)
    if operation == OperationType.FLIP:
        direction = params.get("direction", "horizontal").lower()
        if direction == "vertical":
            return ImageOps.flip(image)
        if direction == "both":
            return ImageOps.flip(ImageOps.mirror(image))
        return ImageOps.mirror(image)
    if operation == OperationType.BLUR:
        radius = float(params.get("radius", "1.5"))
        return image.filter(ImageFilter.GaussianBlur(radius=radius))
    if operation == OperationType.SHARPEN:
        factor = float(params.get("factor", "2.0"))
        return ImageEnhance.Sharpness(image).enhance(factor)
    if operation == OperationType.BRIGHTNESS_CONTRAST:
        brightness = float(params.get("brightness", "1.0"))
        contrast = float(params.get("contrast", "1.0"))
        image = ImageEnhance.Brightness(image).enhance(brightness)
        return ImageEnhance.Contrast(image).enhance(contrast)
    if operation == OperationType.WATERMARK_TEXT:
        draw = ImageDraw.Draw(image)
        draw.text(
            (int(params.get("x", "16")), int(params.get("y", "16"))),
            params.get("text", ""),
            fill=params.get("fill", "white"),
            font=ImageFont.load_default(),
        )
        return image
    if operation == OperationType.FORMAT_CONVERSION:
        return image
    if operation in {OperationType.OCR, OperationType.INFERENCE}:
        raise NotImplementedError(f"{operation.value} is reserved for an extension module")
    raise ValueError(f"unsupported operation: {operation.value}")


def _resize_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    target_width = int(params["width"])
    target_height = int(params["height"])
    return _resize_tiled(task, image, target_width, target_height, params)


def _rotate_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    angle = float(params.get("angle", "0"))
    expand = params.get("expand", "true").lower() == "true"
    if angle == 0:
        return image
    current = image
    step_limit = max(float(params.get("step_degrees", "10")), 1.0)
    steps = max(1, math.ceil(abs(angle) / step_limit))
    step_angle = angle / steps
    for _ in range(steps):
        current = _rotate_tiled(task, current, step_angle, expand, params)
    return current


def _blur_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    radius = float(params.get("radius", "1.5"))
    if radius <= 0:
        return image
    current = image
    max_sub_radius = max(float(params.get("max_sub_radius", "2.0")), 0.5)
    passes = max(1, math.ceil(radius / max_sub_radius))
    pass_radius = radius / math.sqrt(passes)
    for _ in range(passes):
        _step_guard(task, params)
        current = current.filter(ImageFilter.GaussianBlur(radius=pass_radius))
    return current


def _sharpen_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    factor = float(params.get("factor", "2.0"))
    if factor == 1.0:
        return image
    current = image
    passes = max(1, math.ceil(abs(factor - 1.0) / 0.75))
    step_factor = math.pow(factor, 1.0 / passes) if factor > 0 else 1.0
    for _ in range(passes):
        _step_guard(task, params)
        current = ImageEnhance.Sharpness(current).enhance(step_factor)
    return current


def _brightness_contrast_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    brightness = float(params.get("brightness", "1.0"))
    contrast = float(params.get("contrast", "1.0"))
    deviation = max(abs(brightness - 1.0), abs(contrast - 1.0))
    passes = max(1, math.ceil(deviation / 0.5))
    brightness_step = math.pow(brightness, 1.0 / passes) if brightness > 0 else 1.0
    contrast_step = math.pow(contrast, 1.0 / passes) if contrast > 0 else 1.0
    current = image
    for _ in range(passes):
        _step_guard(task, params)
        current = ImageEnhance.Brightness(current).enhance(brightness_step)
        _check_cancelled(task)
        current = ImageEnhance.Contrast(current).enhance(contrast_step)
    return current


def _intermediate_dimension(current: int, target: int) -> int:
    if current == target:
        return target
    if current > target:
        return max(target, current // 2) if current > target * 2 else target
    return min(target, current * 2) if target > current * 2 else target


def _step_guard(task: Task, params: dict[str, str]) -> None:
    _maybe_delay(params)
    _check_cancelled(task)


def _tile_size(params: dict[str, str]) -> int:
    return max(64, int(params.get("tile_size", str(DEFAULT_TILE_SIZE))))


def _resize_tiled(task: Task, image: Image.Image, target_width: int, target_height: int, params: dict[str, str]) -> Image.Image:
    if image.size == (target_width, target_height):
        return image
    output = Image.new(image.mode, (target_width, target_height))
    tile = _tile_size(params)
    scale_x = image.width / target_width
    scale_y = image.height / target_height
    support = 3.0

    for top in range(0, target_height, tile):
        for left in range(0, target_width, tile):
            tile_width = min(tile, target_width - left)
            tile_height = min(tile, target_height - top)
            src_left = max(0, int(math.floor(left * scale_x - support)))
            src_top = max(0, int(math.floor(top * scale_y - support)))
            src_right = min(image.width, int(math.ceil((left + tile_width) * scale_x + support)))
            src_bottom = min(image.height, int(math.ceil((top + tile_height) * scale_y + support)))
            if src_right <= src_left or src_bottom <= src_top:
                continue

            _step_guard(task, params)
            source_tile = image.crop((src_left, src_top, src_right, src_bottom))
            dest_width = max(1, int(round((src_right - src_left) / scale_x)))
            dest_height = max(1, int(round((src_bottom - src_top) / scale_y)))
            resized_tile = source_tile.resize((dest_width, dest_height), resample=Image.Resampling.LANCZOS)

            paste_x = int(round((src_left / scale_x)))
            paste_y = int(round((src_top / scale_y)))
            crop_left = max(0, left - paste_x)
            crop_top = max(0, top - paste_y)
            crop_right = min(resized_tile.width, crop_left + tile_width)
            crop_bottom = min(resized_tile.height, crop_top + tile_height)
            patch = resized_tile.crop((crop_left, crop_top, crop_right, crop_bottom))
            output.paste(patch, (left, top))
    return output


def _rotate_tiled(task: Task, image: Image.Image, angle: float, expand: bool, params: dict[str, str]) -> Image.Image:
    tile = _tile_size(params)
    radians = math.radians(angle)
    cos_a = math.cos(radians)
    sin_a = math.sin(radians)
    in_width, in_height = image.size
    corners = [
        (-in_width / 2.0, -in_height / 2.0),
        (in_width / 2.0, -in_height / 2.0),
        (-in_width / 2.0, in_height / 2.0),
        (in_width / 2.0, in_height / 2.0),
    ]

    if expand:
        rotated = [
            (cos_a * x - sin_a * y, sin_a * x + cos_a * y)
            for x, y in corners
        ]
        xs = [item[0] for item in rotated]
        ys = [item[1] for item in rotated]
        out_width = max(1, int(math.ceil(max(xs) - min(xs))))
        out_height = max(1, int(math.ceil(max(ys) - min(ys))))
    else:
        out_width = in_width
        out_height = in_height

    output = Image.new(image.mode, (out_width, out_height))
    input_center_x = in_width / 2.0
    input_center_y = in_height / 2.0
    output_center_x = out_width / 2.0
    output_center_y = out_height / 2.0

    a = cos_a
    b = sin_a
    c = input_center_x - cos_a * output_center_x - sin_a * output_center_y
    d = -sin_a
    e = cos_a
    f = input_center_y + sin_a * output_center_x - cos_a * output_center_y

    for top in range(0, out_height, tile):
        for left in range(0, out_width, tile):
            tile_width = min(tile, out_width - left)
            tile_height = min(tile, out_height - top)
            _step_guard(task, params)
            coeffs = (
                a,
                b,
                a * left + b * top + c,
                d,
                e,
                d * left + e * top + f,
            )
            patch = image.transform(
                (tile_width, tile_height),
                Image.Transform.AFFINE,
                coeffs,
                resample=Image.Resampling.BICUBIC,
            )
            output.paste(patch, (left, top))
    return output
