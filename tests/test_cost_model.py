from __future__ import annotations

from datetime import UTC, datetime

from worker.models.types import InputImageRef, OperationType, Task, TransformationSpec
from worker.scheduler.cost_model import (
    FILTER_QUEUE_COST_HINTS,
    FORMAT_CONVERSION_QUEUE_COST_HINTS,
    ServiceTimeEstimator,
    estimate_transform_weight,
)


def make_task(*transforms: TransformationSpec, width: int = 1200, height: int = 900, output_format: str = "png") -> Task:
    size_bytes = width * height * 3
    return Task(
        task_id="cost-task",
        idempotency_key="cost-task",
        priority=5,
        created_at=datetime.now(tz=UTC),
        deadline=None,
        max_retries=1,
        transforms=list(transforms),
        input_image=InputImageRef(
            image_id="cost-image",
            payload=None,
            image_format="png",
            size_bytes=size_bytes,
            width=width,
            height=height,
        ),
        output_format=output_format,
    )


def test_filter_queue_cost_hints_cover_business_filters() -> None:
    assert FILTER_QUEUE_COST_HINTS["grayscale"] < FILTER_QUEUE_COST_HINTS["blur"]
    assert FILTER_QUEUE_COST_HINTS["resize"] > FILTER_QUEUE_COST_HINTS["crop"]
    assert FILTER_QUEUE_COST_HINTS["ocr"] > FILTER_QUEUE_COST_HINTS["watermark"]
    assert FILTER_QUEUE_COST_HINTS["inference"] > FILTER_QUEUE_COST_HINTS["ocr"]


def test_rotate_180_costs_less_than_arbitrary_rotation() -> None:
    task_180 = make_task(TransformationSpec(operation=OperationType.ROTATE, params={"angle": "180", "expand": "true"}))
    task_15 = make_task(TransformationSpec(operation=OperationType.ROTATE, params={"angle": "15", "expand": "true"}))

    cost_180 = estimate_transform_weight(task_180, task_180.transforms[0])
    cost_15 = estimate_transform_weight(task_15, task_15.transforms[0])

    assert cost_180 < cost_15


def test_resize_upscale_costs_more_than_downscale() -> None:
    downscale = make_task(TransformationSpec(operation=OperationType.RESIZE, params={"width": "600", "height": "450"}))
    upscale = make_task(TransformationSpec(operation=OperationType.RESIZE, params={"width": "2400", "height": "1800"}))

    downscale_cost = estimate_transform_weight(downscale, downscale.transforms[0])
    upscale_cost = estimate_transform_weight(upscale, upscale.transforms[0])

    assert downscale_cost < upscale_cost


def test_service_time_estimator_penalizes_expensive_pipelines() -> None:
    estimator = ServiceTimeEstimator()
    cheap_task = make_task(TransformationSpec(operation=OperationType.GRAYSCALE))
    expensive_task = make_task(
        TransformationSpec(operation=OperationType.BLUR, params={"radius": "3.5"}),
        TransformationSpec(operation=OperationType.WATERMARK_TEXT, params={"text": "ORG", "size": "48"}),
        TransformationSpec(operation=OperationType.OCR),
    )

    cheap_ms = estimator.estimate(cheap_task)
    expensive_ms = estimator.estimate(expensive_task)

    assert cheap_ms < expensive_ms


def test_format_conversion_queue_cost_hints_are_explicit() -> None:
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["bmp"] == 0.255
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["jpg"] == 0.300
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["png"] == 0.315
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["webp"] == 0.375
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["gif"] == 0.405
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["webp"] > FORMAT_CONVERSION_QUEUE_COST_HINTS["png"]
    assert FORMAT_CONVERSION_QUEUE_COST_HINTS["gif"] > FORMAT_CONVERSION_QUEUE_COST_HINTS["jpg"]
