from __future__ import annotations

from pathlib import Path

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
        key = name.strip().lower()
        payload = args.strip()

        if key in {"grayscale", "gray"}:
            transforms.append(TransformationSpec(operation=OperationType.GRAYSCALE))
        elif key == "resize":
            width, height = _split_pair(payload, "x")
            transforms.append(
                TransformationSpec(
                    operation=OperationType.RESIZE,
                    params={"width": width, "height": height},
                )
            )
        elif key == "crop":
            left, upper, right, lower = _split_list(payload, 4)
            transforms.append(
                TransformationSpec(
                    operation=OperationType.CROP,
                    params={"left": left, "upper": upper, "right": right, "lower": lower},
                )
            )
        elif key == "rotate":
            angle, expand = _parse_angle(payload)
            transforms.append(
                TransformationSpec(
                    operation=OperationType.ROTATE,
                    params={"angle": angle, "expand": expand},
                )
            )
        elif key == "flip":
            transforms.append(
                TransformationSpec(operation=OperationType.FLIP, params={"direction": payload or "horizontal"})
            )
        elif key == "blur":
            transforms.append(
                TransformationSpec(operation=OperationType.BLUR, params={"radius": payload or "1.5"})
            )
        elif key == "sharpen":
            transforms.append(
                TransformationSpec(operation=OperationType.SHARPEN, params={"factor": payload or "2.0"})
            )
        elif key == "brightness":
            transforms.append(
                TransformationSpec(
                    operation=OperationType.BRIGHTNESS_CONTRAST,
                    params={"brightness": payload or "1.0", "contrast": "1.0"},
                )
            )
        elif key == "contrast":
            transforms.append(
                TransformationSpec(
                    operation=OperationType.BRIGHTNESS_CONTRAST,
                    params={"brightness": "1.0", "contrast": payload or "1.0"},
                )
            )
        elif key in {"brightness_contrast", "brightness-contrast"}:
            brightness, contrast = _split_list(payload, 2)
            transforms.append(
                TransformationSpec(
                    operation=OperationType.BRIGHTNESS_CONTRAST,
                    params={"brightness": brightness, "contrast": contrast},
                )
            )
        elif key in {"watermark", "watermark_text"}:
            transforms.append(
                TransformationSpec(
                    operation=OperationType.WATERMARK_TEXT,
                    params=_parse_watermark(payload),
                )
            )
        elif key in {"format", "format_conversion", "convert"}:
            output_format = payload.lower()
            transforms.append(TransformationSpec(operation=OperationType.FORMAT_CONVERSION))
        elif key == "ocr":
            transforms.append(TransformationSpec(operation=OperationType.OCR))
        elif key == "inference":
            transforms.append(TransformationSpec(operation=OperationType.INFERENCE))
        else:
            raise ValueError(f"unsupported filter syntax: {raw_filter}")

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
