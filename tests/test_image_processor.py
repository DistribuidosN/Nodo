from __future__ import annotations

import json
import sys
import threading
import time
from io import BytesIO
from pathlib import Path
from datetime import UTC, datetime

from PIL import Image, ImageChops

import pytest

from worker.core.storage import StorageClient
from worker.execution.image_processor import TaskCancelledError, process_image
from worker.models.types import InputImageRef, OperationType, Task, TransformationSpec


def make_payload() -> bytes:
    image = Image.new("RGB", (120, 90), color=(200, 80, 10))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def test_process_image_pipeline(tmp_path):
    payload = make_payload()
    task = Task(
        task_id="pipeline",
        idempotency_key="pipeline",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[
            TransformationSpec(operation=OperationType.GRAYSCALE),
            TransformationSpec(operation=OperationType.RESIZE, params={"width": "60", "height": "45"}),
            TransformationSpec(operation=OperationType.FORMAT_CONVERSION),
        ],
        input_image=InputImageRef(
            image_id="img-1",
            payload=payload,
            image_format="png",
            size_bytes=len(payload),
            width=120,
            height=90,
        ),
        output_format="jpg",
    )

    output_path, output_format, width, height, size_bytes, metadata = process_image(task, str(tmp_path))
    assert output_format == "jpg"
    assert width == 60
    assert height == 45
    assert size_bytes > 0
    assert metadata["processor"] == "pillow"
    assert tmp_path.joinpath("pipeline.jpg").exists()


def test_process_image_cancels_mid_resize(tmp_path):
    image = Image.new("RGB", (2048, 2048), color=(50, 90, 180))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    payload = buffer.getvalue()
    token_path = tmp_path / "mid.cancel"

    task = Task(
        task_id="mid-cancel",
        idempotency_key="mid-cancel",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[
            TransformationSpec(
                operation=OperationType.RESIZE,
                params={"width": "512", "height": "512", "delay_ms": "20", "tile_size": "64"},
            )
        ],
        input_image=InputImageRef(
            image_id="img-mid",
            payload=payload,
            image_format="png",
            size_bytes=len(payload),
            width=2048,
            height=2048,
        ),
        output_format="png",
        cancel_token_path=str(token_path),
    )

    def trigger_cancel() -> None:
        time.sleep(0.08)
        token_path.write_text("cancel", encoding="utf-8")

    thread = threading.Thread(target=trigger_cancel, daemon=True)
    thread.start()
    with pytest.raises(TaskCancelledError):
        process_image(task, str(tmp_path))
    thread.join(timeout=1.0)


def test_process_image_supports_input_uri_and_local_output(tmp_path):
    payload = make_payload()
    storage = StorageClient()
    source_path = tmp_path / "shared" / "source.png"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(payload)

    task = Task(
        task_id="uri-pipeline",
        idempotency_key="uri-pipeline",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[
            TransformationSpec(operation=OperationType.RESIZE, params={"width": "30", "height": "20"}),
        ],
        input_image=InputImageRef(
            image_id="img-uri",
            input_uri=source_path.resolve().as_uri(),
            image_format="png",
            size_bytes=len(payload),
            width=120,
            height=90,
        ),
        output_format="png",
    )

    output_path, output_format, width, height, _size_bytes, metadata = process_image(
        task,
        str(tmp_path / "local-out"),
        storage,
    )

    assert Path(output_path).exists()
    assert output_format == "png"
    assert width == 30
    assert height == 20
    assert metadata["output_backend"] == "filesystem"


def test_process_image_rotate_ninety_keeps_expected_dimensions(tmp_path):
    payload = make_payload()
    task = Task(
        task_id="rotate-90",
        idempotency_key="rotate-90",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[TransformationSpec(operation=OperationType.ROTATE, params={"angle": "90", "expand": "true"})],
        input_image=InputImageRef(
            image_id="img-rotate-90",
            payload=payload,
            image_format="png",
            size_bytes=len(payload),
            width=120,
            height=90,
        ),
        output_format="png",
    )

    output_path, output_format, width, height, size_bytes, _metadata = process_image(task, str(tmp_path))

    assert output_format == "png"
    assert width == 90
    assert height == 120
    assert size_bytes > 0
    saved = Image.open(output_path)
    assert saved.size == (90, 120)


def test_process_image_watermark_changes_pixels_on_light_background(tmp_path):
    image = Image.new("RGB", (600, 360), color=(245, 245, 245))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    payload = buffer.getvalue()

    task = Task(
        task_id="watermark-visible",
        idempotency_key="watermark-visible",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[
            TransformationSpec(
                operation=OperationType.WATERMARK_TEXT,
                params={
                    "text": "Demo Nodo",
                    "x": "20",
                    "y": "20",
                    "fill": "white",
                    "size": "42",
                    "stroke_width": "2",
                    "opacity": "96",
                    "angle": "-28",
                    "spacing_x": "220",
                    "spacing_y": "150",
                },
            )
        ],
        input_image=InputImageRef(
            image_id="img-watermark",
            payload=payload,
            image_format="png",
            size_bytes=len(payload),
            width=600,
            height=360,
        ),
        output_format="png",
    )

    output_path, _output_format, _width, _height, _size_bytes, _metadata = process_image(task, str(tmp_path))

    saved = Image.open(output_path).convert("RGB")
    diff = ImageChops.difference(image, saved)
    bbox = diff.getbbox()
    assert bbox is not None
    assert bbox[0] < image.width // 4
    assert bbox[1] < image.height // 4
    assert bbox[2] > (image.width * 3) // 4
    assert bbox[3] > (image.height * 3) // 4


def test_process_image_runs_ocr_and_inference_adapters(tmp_path):
    payload = make_payload()
    adapter_script = (
        "import json, os;"
        "operation=os.environ['WORKER_OPERATION'];"
        "output=os.environ['WORKER_OUTPUT_JSON'];"
        "payload={'metadata': {f'{operation}_engine': 'stub'}};"
        "payload['text']='hello world' if operation=='ocr' else payload.setdefault('labels', ['sample']);"
        "payload['score']=0.99 if operation=='inference' else payload.get('score');"
        "open(output, 'w', encoding='utf-8').write(json.dumps(payload))"
    )
    command = json.dumps([sys.executable, "-c", adapter_script])

    task = Task(
        task_id="analysis-pipeline",
        idempotency_key="analysis-pipeline",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[
            TransformationSpec(operation=OperationType.OCR),
            TransformationSpec(operation=OperationType.INFERENCE),
        ],
        input_image=InputImageRef(
            image_id="img-analysis",
            payload=payload,
            image_format="png",
            size_bytes=len(payload),
            width=120,
            height=90,
        ),
        output_format="png",
        metadata={"ocr_command": command, "inference_command": command},
    )

    _output_path, _output_format, _width, _height, _size_bytes, metadata = process_image(task, str(tmp_path))

    assert metadata["ocr_text"] == "hello world"
    assert metadata["ocr_ocr_engine"] == "stub"
    assert metadata["inference_labels"] == "[\"sample\"]"
    assert metadata["inference_score"] == "0.99"


@pytest.mark.parametrize(
    ("output_format", "expected_save_format", "expected_mode"),
    [
        ("webp", "WEBP", "RGBA"),
        ("bmp", "BMP", "RGB"),
        ("gif", "GIF", "P"),
        ("ico", "ICO", "RGBA"),
    ],
)
def test_process_image_supports_additional_output_formats(tmp_path, output_format, expected_save_format, expected_mode):
    image = Image.new("RGBA", (96, 96), color=(120, 50, 210, 180))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    payload = buffer.getvalue()

    task = Task(
        task_id=f"format-{output_format}",
        idempotency_key=f"format-{output_format}",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=0,
        transforms=[TransformationSpec(operation=OperationType.FORMAT_CONVERSION)],
        input_image=InputImageRef(
            image_id=f"img-{output_format}",
            payload=payload,
            image_format="png",
            size_bytes=len(payload),
            width=96,
            height=96,
        ),
        output_format=output_format,
    )

    output_path, actual_format, width, height, size_bytes, _metadata = process_image(task, str(tmp_path))
    assert actual_format == output_format
    assert width == 96
    assert height == 96
    assert size_bytes > 0
    assert Path(output_path).suffix == f".{output_format}"

    saved = Image.open(output_path)
    assert saved.format == expected_save_format
    assert saved.mode == expected_mode
