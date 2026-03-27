from __future__ import annotations

import threading
import time
from io import BytesIO
from datetime import UTC, datetime

from PIL import Image

import pytest

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
