from __future__ import annotations

from pathlib import Path
from typing import Callable

from worker.models.types import OperationType, TransformationSpec


def infer_output_format(file_name: str | None, fallback: str = "png") -> str:
    if not file_name:
        return fallback
    suffix = Path(file_name).suffix.lstrip(".").lower()
    return suffix or fallback


def parse_filters(filters: list[str]) -> tuple[list[TransformationSpec], str | None]:
    transforms: list[TransformationSpec] = []
    output_format: str | None = None

    for raw_filter in filters:
        value = raw_filter.strip()
        if not value:
            continue
        name, _, args = value.partition(":")
        key = _canonical_key(name.strip().lower())
        payload = args.strip()

        if key == "format":
            output_format = payload.lower()
            transforms.append(TransformationSpec(operation=OperationType.FORMAT_CONVERSION))
            continue
        builder: BuilderFn | None = _TRANSFORM_BUILDERS.get(key)
        if not builder:
            raise ValueError(f"unsupported filter syntax: {raw_filter}")
        transforms.append(builder(payload))

    return transforms, output_format


def _split_pair(value: str, separator: str) -> tuple[str, str]:
    left, _, right = value.partition(separator)
    if not left or not right:
        raise ValueError(f"expected two values separated by '{separator}' in '{value}'")
    return left.strip(), right.strip()


def _split_list(value: str, expected: int) -> list[str]:
    parts = [item.strip() for item in value.split(",") if item.strip()]
    if len(parts) != expected:
        raise ValueError(f"expected {expected} comma-separated values in '{value}'")
    return parts


def _parse_angle(value: str) -> tuple[str, str]:
    if "," not in value:
        return value or "0", "true"
    angle, expand = _split_list(value, 2)
    return angle, expand.lower()


def _parse_watermark(value: str) -> dict[str, str]:
    if not value:
        return {"text": "", "x": "16", "y": "16", "fill": "white"}
    parts = [item.strip() for item in value.split("|")]
    text = parts[0]
    x = parts[1] if len(parts) > 1 and parts[1] else "16"
    y = parts[2] if len(parts) > 2 and parts[2] else "16"
    fill = parts[3] if len(parts) > 3 and parts[3] else "white"
    return {"text": text, "x": x, "y": y, "fill": fill}


BuilderFn = Callable[[str], TransformationSpec]


def _canonical_key(key: str) -> str:
    if key in {"gray", "grayscale"}:
        return "grayscale"
    if key in {"brightness_contrast", "brightness-contrast"}:
        return "brightness_contrast"
    if key in {"watermark", "watermark_text"}:
        return "watermark_text"
    if key in {"format", "format_conversion", "convert"}:
        return "format"
    return key


def _build_grayscale(_: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.GRAYSCALE)


def _build_resize(payload: str) -> TransformationSpec:
    width, height = _split_pair(payload, "x")
    return TransformationSpec(operation=OperationType.RESIZE, params={"width": width, "height": height})


def _build_crop(payload: str) -> TransformationSpec:
    left, upper, right, lower = _split_list(payload, 4)
    return TransformationSpec(
        operation=OperationType.CROP, params={"left": left, "upper": upper, "right": right, "lower": lower}
    )


def _build_rotate(payload: str) -> TransformationSpec:
    angle, expand = _parse_angle(payload)
    return TransformationSpec(operation=OperationType.ROTATE, params={"angle": angle, "expand": expand})


def _build_flip(payload: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.FLIP, params={"direction": payload or "horizontal"})


def _build_blur(payload: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.BLUR, params={"radius": payload or "1.5"})


def _build_sharpen(payload: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.SHARPEN, params={"factor": payload or "2.0"})


def _build_brightness(payload: str) -> TransformationSpec:
    return TransformationSpec(
        operation=OperationType.BRIGHTNESS_CONTRAST, params={"brightness": payload or "1.0", "contrast": "1.0"}
    )


def _build_contrast(payload: str) -> TransformationSpec:
    return TransformationSpec(
        operation=OperationType.BRIGHTNESS_CONTRAST, params={"brightness": "1.0", "contrast": payload or "1.0"}
    )


def _build_brightness_contrast(payload: str) -> TransformationSpec:
    brightness, contrast = _split_list(payload, 2)
    return TransformationSpec(
        operation=OperationType.BRIGHTNESS_CONTRAST, params={"brightness": brightness, "contrast": contrast}
    )


def _build_watermark_text(payload: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.WATERMARK_TEXT, params=_parse_watermark(payload))


def _build_ocr(_: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.OCR)


def _build_inference(_: str) -> TransformationSpec:
    return TransformationSpec(operation=OperationType.INFERENCE)


_TRANSFORM_BUILDERS: dict[str, BuilderFn] = {
    "grayscale": _build_grayscale,
    "resize": _build_resize,
    "crop": _build_crop,
    "rotate": _build_rotate,
    "flip": _build_flip,
    "blur": _build_blur,
    "sharpen": _build_sharpen,
    "brightness": _build_brightness,
    "contrast": _build_contrast,
    "brightness_contrast": _build_brightness_contrast,
    "watermark_text": _build_watermark_text,
    "ocr": _build_ocr,
    "inference": _build_inference,
}
