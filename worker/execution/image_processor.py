from __future__ import annotations

import json
import math
import os
import subprocess
import tempfile
import time
from io import BytesIO
from pathlib import Path
from typing import cast

from PIL import Image, ImageColor, ImageDraw, ImageEnhance, ImageFilter, ImageFont, ImageOps

from worker.core.storage import StorageClient
from worker.models.types import OperationType, Task


FORMAT_MAP = {
    "jpg": "JPEG",
    "jpeg": "JPEG",
    "png": "PNG",
    "tif": "TIFF",
    "tiff": "TIFF",
    "webp": "WEBP",
    "bmp": "BMP",
    "gif": "GIF",
    "ico": "ICO",
}

DEFAULT_TILE_SIZE = 256
INTERNAL_RESULT_METADATA_KEYS = {
    "node_id",
    "execution_isolation",
    "child_pid",
    "ocr_command",
    "inference_command",
    "adapter_timeout_seconds",
}


class TaskCancelledError(Exception):
    """Raised when a running task observes a cancellation token."""


def process_image(
    task: Task,
    output_dir: str,
    storage: StorageClient | None = None,
) -> tuple[str, str, int, int, int, dict[str, str]]:
    storage_client = storage or StorageClient()
    _check_cancelled(task)
    image = _load_image(task, storage_client)
    for transform in task.transforms:
        image = apply_transform_checked(task, image, transform.operation, transform.params)

    requested_format = (task.output_format or task.input_image.image_format or "png").lower()
    output_bytes, _ = serialize_image(image, requested_format)
    width, height = image.size
    output_path, size_bytes = persist_output_bytes(
        task.task_id,
        output_bytes,
        requested_format,
        output_dir,
    )
    metadata = _result_metadata(task, processor="pillow", output_backend=_output_backend(output_path))
    return str(output_path), requested_format, width, height, size_bytes, metadata


def process_transform_stage(
    task: Task,
    payload: bytes,
    operation: OperationType,
    params: dict[str, str],
    stage_output_format: str = "png",
) -> tuple[bytes, str, int, int, dict[str, str]]:
    _check_cancelled(task)
    image = load_image_bytes(payload)
    image = apply_transform_checked(task, image, operation, params)
    output_bytes, _save_format = serialize_image(image, stage_output_format)
    width, height = image.size
    metadata = _result_metadata(task, processor="pillow", stage_operation=operation.value)
    return output_bytes, stage_output_format, width, height, metadata


def load_input_bytes(task: Task, storage: StorageClient | None = None) -> tuple[bytes, str]:
    storage_client = storage or StorageClient()
    if task.input_image.payload:
        return task.input_image.payload, (task.input_image.image_format or "png").lower()
    if task.input_image.input_path:
        path = Path(task.input_image.input_path)
        return path.read_bytes(), (task.input_image.image_format or path.suffix.lstrip(".") or "png").lower()
    if task.input_image.input_uri:
        payload = storage_client.read_bytes(task.input_image.input_uri)
        parsed_suffix = Path(task.input_image.input_uri).suffix.lstrip(".")
        image_format = (task.input_image.image_format or parsed_suffix or "png").lower()
        return payload, image_format
    raise ValueError("task does not contain an image source")


def load_image_bytes(payload: bytes) -> Image.Image:
    return Image.open(BytesIO(payload)).copy()


def serialize_image(image: Image.Image, requested_format: str) -> tuple[bytes, str]:
    requested_format = requested_format.lower()
    save_format = FORMAT_MAP.get(requested_format, requested_format.upper())
    image = _normalize_image_for_format(image, save_format)
    buffer = BytesIO()
    image.save(buffer, format=save_format)
    return buffer.getvalue(), save_format


def persist_output_bytes(
    task_id: str,
    output_bytes: bytes,
    requested_format: str,
    output_dir: str,
) -> tuple[str, int]:
    output_path = Path(output_dir) / f"{task_id}.{requested_format}"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    tmp_path.write_bytes(output_bytes)
    tmp_path.replace(output_path)
    return str(output_path), output_path.stat().st_size


def _load_image(task: Task, storage: StorageClient) -> Image.Image:
    payload, _input_format = load_input_bytes(task, storage)
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
    return _apply_transform(image_task, image, operation, params)


def _apply_transform(task: Task, image: Image.Image, operation: OperationType, params: dict[str, str]) -> Image.Image:
    if operation == OperationType.GRAYSCALE:
        return ImageOps.grayscale(image)
    if operation == OperationType.RESIZE:
        width = int(params["width"])
        height = int(params["height"])
        return image.resize((width, height), resample=Image.Resampling.LANCZOS)
    if operation == OperationType.CROP:
        left = float(params["left"])
        upper = float(params["upper"])
        right = float(params["right"])
        lower = float(params["lower"])
        box: tuple[float, float, float, float] = (left, upper, right, lower)
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
        return _apply_watermark_text(image, params)
    if operation == OperationType.FORMAT_CONVERSION:
        return image
    if operation in {OperationType.OCR, OperationType.INFERENCE}:
        return _run_adapter_operation(task, image, operation, params)
    raise ValueError(f"unsupported operation: {operation.value}")


def _resize_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    target_width = int(params["width"])
    target_height = int(params["height"])
    return _resize_tiled(task, image, target_width, target_height, params)


def _rotate_cooperative(task: Task, image: Image.Image, params: dict[str, str]) -> Image.Image:
    angle = float(params.get("angle", "0"))
    expand = params.get("expand", "true").lower() == "true"
    if math.isclose(angle, 0.0, abs_tol=1e-9):
        return image
    if exact_quadrant := _exact_quadrant_rotation(angle, expand):
        _step_guard(task, params)
        return image.transpose(exact_quadrant)
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
    if math.isclose(factor, 1.0, abs_tol=1e-9):
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


def _step_guard(task: Task, params: dict[str, str]) -> None:
    _maybe_delay(params)
    _check_cancelled(task)


def _tile_size(params: dict[str, str]) -> int:
    return max(64, int(params.get("tile_size", str(DEFAULT_TILE_SIZE))))


def _exact_quadrant_rotation(angle: float, expand: bool) -> Image.Transpose | None:
    if not expand:
        return None
    normalized = int(round(angle)) % 360
    if not math.isclose(angle, normalized, abs_tol=1e-9):
        return None
    mapping: dict[int, Image.Transpose] = {
        90: Image.Transpose.ROTATE_90,
        180: Image.Transpose.ROTATE_180,
        270: Image.Transpose.ROTATE_270,
    }
    return mapping.get(normalized)


def _load_watermark_font(size: int) -> ImageFont.ImageFont | ImageFont.FreeTypeFont:
    font_size = max(12, size)
    try:
        return ImageFont.truetype("DejaVuSans.ttf", size=font_size)
    except OSError:
        return ImageFont.load_default()


def _watermark_stroke_fill(fill: str) -> str:
    normalized = fill.strip().lower()
    dark_fills = {"black", "#000", "#000000", "navy", "blue", "purple", "maroon", "brown"}
    return "white" if normalized in dark_fills else "black"


def _apply_watermark_text(image: Image.Image, params: dict[str, str]) -> Image.Image:
    text = params.get("text", "").strip()
    if not text:
        return image

    size = max(12, int(params.get("size", "36")))
    stroke_width = max(0, int(params.get("stroke_width", "2")))
    opacity = max(16, min(200, int(params.get("opacity", "96"))))
    angle = float(params.get("angle", "-28"))
    offset_x = int(params.get("x", "16"))
    offset_y = int(params.get("y", "16"))
    font = _load_watermark_font(size)

    fill = params.get("fill", "white")
    fill_rgba = _color_with_alpha(fill, opacity)
    stroke_rgba = _color_with_alpha(_watermark_stroke_fill(fill), min(255, opacity + 48))
    stamp = _build_watermark_stamp(text, font, fill_rgba, stroke_rgba, stroke_width, angle)

    spacing_x = max(stamp.width // 2, int(params.get("spacing_x", str(max(220, size * 5)))))
    spacing_y = max(stamp.height // 2, int(params.get("spacing_y", str(max(160, size * 4)))))

    base = image.convert("RGBA")
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    row_index = 0
    start_y = -stamp.height + offset_y
    end_y = overlay.height + stamp.height
    start_x = -stamp.width + offset_x
    end_x = overlay.width + stamp.width

    for top in range(start_y, end_y, spacing_y):
        row_shift = spacing_x // 2 if row_index % 2 else 0
        for left in range(start_x + row_shift, end_x, spacing_x):
            overlay.paste(stamp, (left, top), stamp)
        row_index += 1

    result = Image.alpha_composite(base, overlay)
    return _restore_image_mode(result, image.mode)


def _build_watermark_stamp(
    text: str,
    font: ImageFont.ImageFont | ImageFont.FreeTypeFont,
    fill_rgba: tuple[int, int, int, int],
    stroke_rgba: tuple[int, int, int, int],
    stroke_width: int,
    angle: float,
) -> Image.Image:
    dummy = Image.new("RGBA", (1, 1), (0, 0, 0, 0))
    draw = ImageDraw.Draw(dummy)
    left, top, right, bottom = draw.textbbox((0, 0), text, font=font, stroke_width=stroke_width)
    width = max(1, right - left)
    height = max(1, bottom - top)
    padding = max(18, stroke_width * 6)

    stamp = Image.new("RGBA", (width + padding * 2, height + padding * 2), (0, 0, 0, 0))
    stamp_draw = ImageDraw.Draw(stamp)
    stamp_draw.text(
        (padding - left, padding - top),
        text,
        fill=fill_rgba,
        font=font,
        stroke_width=stroke_width,
        stroke_fill=stroke_rgba,
    )
    return stamp.rotate(angle, expand=True, resample=Image.Resampling.BICUBIC)


def _color_with_alpha(fill: str, opacity: int) -> tuple[int, int, int, int]:
    try:
        rgba = ImageColor.getcolor(fill, "RGBA")
    except ValueError:
        rgba = ImageColor.getcolor("white", "RGBA")
    return rgba[:3] + (opacity,)


def _restore_image_mode(image: Image.Image, original_mode: str) -> Image.Image:
    if original_mode == image.mode:
        return image
    if original_mode in {"RGB", "L", "RGBA"}:
        return image.convert(original_mode)
    return image


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
        rotated = [(cos_a * x - sin_a * y, sin_a * x + cos_a * y) for x, y in corners]
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


def _run_adapter_operation(task: Task, image: Image.Image, operation: OperationType, params: dict[str, str]) -> Image.Image:
    command = params.get("command") or task.metadata.get(f"{operation.value}_command")
    if not command:
        raise ValueError(f"{operation.value} command is not configured")

    timeout = float(params.get("timeout_seconds") or task.metadata.get("adapter_timeout_seconds") or "30")
    with tempfile.TemporaryDirectory(prefix=f"{operation.value}-{task.task_id}-") as temp_dir:
        temp_path = Path(temp_dir)
        input_path = temp_path / "input.png"
        output_image = temp_path / "output.png"
        output_json = temp_path / "result.json"
        image.save(input_path, format="PNG")

        env = os.environ.copy()
        env.update(
            {
                "WORKER_INPUT_FILE": str(input_path),
                "WORKER_OUTPUT_IMAGE": str(output_image),
                "WORKER_OUTPUT_JSON": str(output_json),
                "WORKER_OPERATION": operation.value,
                "WORKER_TASK_ID": task.task_id,
                "WORKER_IMAGE_ID": task.input_image.image_id,
                "WORKER_PARAMS_JSON": json.dumps(params, ensure_ascii=True),
            }
        )
        completed = _run_command(command, env, timeout)
        adapter_payload = _load_adapter_payload(output_json, completed.stdout)
        if adapter_payload is not None:
            _merge_adapter_metadata(task, operation, adapter_payload)

        _check_cancelled(task)
        if output_image.exists():
            return Image.open(output_image).copy()
    return image


def _run_command(command: str, env: dict[str, str], timeout: float) -> subprocess.CompletedProcess[str]:
    command = command.strip()
    if command.startswith("["):
        argv = json.loads(command)
        completed = subprocess.run(argv, check=False, capture_output=True, text=True, env=env, timeout=timeout)
    else:
        completed = subprocess.run(command, shell=True, check=False, capture_output=True, text=True, env=env, timeout=timeout)

    if completed.returncode != 0:
        stderr = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(stderr or f"adapter command failed with code {completed.returncode}")
    return completed


def _load_adapter_payload(output_json: Path, stdout: str) -> dict[str, object] | str | None:
    if output_json.exists():
        return json.loads(output_json.read_text(encoding="utf-8"))
    stdout = stdout.strip()
    if not stdout:
        return None
    try:
        return json.loads(stdout)
    except json.JSONDecodeError:
        return stdout


def _merge_adapter_metadata(task: Task, operation: OperationType, payload: dict[str, object] | str) -> None:
    prefix = operation.value
    if isinstance(payload, str):
        task.metadata[f"{prefix}_text"] = payload
        return

    metadata_obj = payload.get("metadata")
    if isinstance(metadata_obj, dict):
        metadata_dict = cast(dict[str, object], metadata_obj)
        for key, value in metadata_dict.items():
            task.metadata[f"{prefix}_{key}"] = _stringify(value)

    for key in ("text", "artifact_uri", "artifact_path", "score", "label"):
        if key in payload:
            task.metadata[f"{prefix}_{key}"] = _stringify(payload[key])

    if "labels" in payload:
        task.metadata[f"{prefix}_labels"] = json.dumps(payload["labels"], ensure_ascii=True)


def _result_metadata(task: Task, **extras: str) -> dict[str, str]:
    metadata = {
        key: value
        for key, value in task.metadata.items()
        if key not in INTERNAL_RESULT_METADATA_KEYS
    }
    metadata.update(extras)
    return metadata


def _output_backend(output_path: str) -> str:
    return "uri" if "://" in output_path else "filesystem"


def _normalize_image_for_format(image: Image.Image, save_format: str) -> Image.Image:
    if save_format == "JPEG" and image.mode not in ("RGB", "L"):
        return image.convert("RGB")
    if save_format == "BMP" and image.mode == "P":
        return image.convert("RGB")
    if save_format == "GIF":
        if image.mode in {"P", "L"}:
            return image
        return image.convert("P", palette=Image.Palette.ADAPTIVE)
    if save_format == "ICO" and image.mode not in {"RGBA", "RGB", "L"}:
        return image.convert("RGBA")
    return image


def _stringify(value: object) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=True) if isinstance(value, (dict, list)) else str(value)
