from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from google.protobuf.timestamp_pb2 import Timestamp

from proto import worker_node_pb2
from worker.telemetry.tracing import strip_internal_trace_metadata
from worker.models.types import (
    ExecutionResultRecord,
    InputImageRef,
    NodeMode,
    NodeState,
    OperationType,
    ProgressEventRecord,
    Task,
    TaskState,
    TransformationSpec,
)


PROTO = cast(Any, worker_node_pb2)


TASK_STATUS_TO_PROTO: dict[TaskState, int] = {
    TaskState.QUEUED: PROTO.TASK_STATUS_QUEUED,
    TaskState.ADMITTED: PROTO.TASK_STATUS_ADMITTED,
    TaskState.RUNNING: PROTO.TASK_STATUS_RUNNING,
    TaskState.RETRY_SCHEDULED: PROTO.TASK_STATUS_RETRY_SCHEDULED,
    TaskState.SUCCEEDED: PROTO.TASK_STATUS_SUCCEEDED,
    TaskState.FAILED: PROTO.TASK_STATUS_FAILED,
    TaskState.CANCELLED: PROTO.TASK_STATUS_CANCELLED,
    TaskState.REJECTED: PROTO.TASK_STATUS_REJECTED,
}

NODE_MODE_TO_PROTO: dict[NodeMode, int] = {
    NodeMode.ACTIVE: PROTO.NODE_MODE_ACTIVE,
    NodeMode.DRAINING: PROTO.NODE_MODE_DRAINING,
    NodeMode.SHUTTING_DOWN: PROTO.NODE_MODE_SHUTTING_DOWN,
}

IMAGE_FORMAT_FROM_PROTO: dict[int, str] = {
    PROTO.IMAGE_FORMAT_JPG: "jpg",
    PROTO.IMAGE_FORMAT_PNG: "png",
    PROTO.IMAGE_FORMAT_TIF: "tif",
    PROTO.IMAGE_FORMAT_WEBP: "webp",
    PROTO.IMAGE_FORMAT_BMP: "bmp",
    PROTO.IMAGE_FORMAT_GIF: "gif",
    PROTO.IMAGE_FORMAT_ICO: "ico",
}

IMAGE_FORMAT_TO_PROTO: dict[str, int] = {
    "jpg": PROTO.IMAGE_FORMAT_JPG,
    "jpeg": PROTO.IMAGE_FORMAT_JPG,
    "png": PROTO.IMAGE_FORMAT_PNG,
    "tif": PROTO.IMAGE_FORMAT_TIF,
    "tiff": PROTO.IMAGE_FORMAT_TIF,
    "webp": PROTO.IMAGE_FORMAT_WEBP,
    "bmp": PROTO.IMAGE_FORMAT_BMP,
    "gif": PROTO.IMAGE_FORMAT_GIF,
    "ico": PROTO.IMAGE_FORMAT_ICO,
}

OPERATION_FROM_PROTO: dict[int, OperationType] = {
    PROTO.OPERATION_GRAYSCALE: OperationType.GRAYSCALE,
    PROTO.OPERATION_RESIZE: OperationType.RESIZE,
    PROTO.OPERATION_CROP: OperationType.CROP,
    PROTO.OPERATION_ROTATE: OperationType.ROTATE,
    PROTO.OPERATION_FLIP: OperationType.FLIP,
    PROTO.OPERATION_BLUR: OperationType.BLUR,
    PROTO.OPERATION_SHARPEN: OperationType.SHARPEN,
    PROTO.OPERATION_BRIGHTNESS_CONTRAST: OperationType.BRIGHTNESS_CONTRAST,
    PROTO.OPERATION_WATERMARK_TEXT: OperationType.WATERMARK_TEXT,
    PROTO.OPERATION_FORMAT_CONVERSION: OperationType.FORMAT_CONVERSION,
    PROTO.OPERATION_OCR: OperationType.OCR,
    PROTO.OPERATION_INFERENCE: OperationType.INFERENCE,
}


def timestamp_from_datetime(value: datetime | None) -> Timestamp:
    result = Timestamp()
    if value is not None:
        result.FromDatetime(value.astimezone(UTC))
    return result


def datetime_from_timestamp(value: Timestamp) -> datetime | None:
    if value.seconds == 0 and value.nanos == 0:
        return None
    return value.ToDatetime(tzinfo=UTC)


def task_from_proto(message: Any) -> Task:
    proto_input = message.input
    source = cast(str | None, proto_input.WhichOneof("source"))
    image = InputImageRef(
        image_id=str(proto_input.image_id),
        input_path=str(proto_input.input_path) if source == "input_path" else None,
        input_uri=str(proto_input.input_uri) if source == "input_uri" else None,
        payload=bytes(proto_input.content) if source == "content" else None,
        image_format=IMAGE_FORMAT_FROM_PROTO.get(int(proto_input.format), "png"),
        size_bytes=int(proto_input.size_bytes),
        width=int(proto_input.width),
        height=int(proto_input.height),
    )
    transforms: list[TransformationSpec] = [
        TransformationSpec(
            operation=OPERATION_FROM_PROTO[int(item.type)],
            params=dict(cast(dict[str, str], item.params)),
        )
        for item in message.transforms
    ]
    task_payload: dict[str, Any] = {
        "task_id": str(message.task_id),
        "idempotency_key": str(message.idempotency_key or message.task_id),
        "priority": int(message.priority),
        "created_at": datetime_from_timestamp(cast(Timestamp, message.created_at)) or datetime.now(tz=UTC),
        "deadline": datetime_from_timestamp(cast(Timestamp, message.deadline)),
        "max_retries": int(message.max_retries),
        "transforms": transforms,
        "input_image": image,
        "output_format": IMAGE_FORMAT_FROM_PROTO.get(int(message.output_format), image.image_format or "png"),
        "metadata": dict(cast(dict[str, str], message.metadata)),
    }
    return Task(**task_payload)


def progress_to_proto(event: ProgressEventRecord) -> Any:
    return PROTO.ProgressEvent(
        task_id=event.task_id,
        image_id=event.image_id,
        node_id=event.node_id,
        status=TASK_STATUS_TO_PROTO[event.state],
        progress_pct=event.progress_pct,
        attempt=event.attempt,
        queue_wait_ms=event.queue_wait_ms,
        run_time_ms=event.run_time_ms,
        message=event.message,
        timestamp=timestamp_from_datetime(event.timestamp),
        metadata=strip_internal_trace_metadata(event.metadata),
    )


def result_to_proto(result: ExecutionResultRecord) -> Any:
    return PROTO.ExecutionResult(
        task_id=result.task_id,
        image_id=result.image_id,
        node_id=result.node_id,
        status=TASK_STATUS_TO_PROTO[result.state],
        attempt=result.attempt,
        output_path=result.output_path or "",
        output_format=IMAGE_FORMAT_TO_PROTO.get((result.output_format or "png").lower(), PROTO.IMAGE_FORMAT_PNG),
        width=result.width,
        height=result.height,
        size_bytes=result.size_bytes,
        error_code=result.error_code or "",
        error_message=result.error_message or "",
        started_at=timestamp_from_datetime(result.started_at),
        finished_at=timestamp_from_datetime(result.finished_at),
        metadata=strip_internal_trace_metadata(result.metadata),
    )


def node_status_to_proto(state: NodeState) -> Any:
    return PROTO.NodeStatus(
        node_id=state.node_id,
        mode=NODE_MODE_TO_PROTO[state.mode],
        accepting_tasks=state.accepting_tasks,
        queue_length=state.queue_length,
        active_tasks=state.active_tasks,
        capacity_effective=state.capacity_effective,
        cpu_utilization=state.cpu_utilization,
        memory_utilization=state.memory_utilization,
        gpu_utilization=state.gpu_utilization,
        io_utilization=state.io_utilization,
        available_memory_bytes=state.available_memory_bytes,
        reserved_memory_bytes=state.reserved_memory_bytes,
        timestamp=timestamp_from_datetime(state.timestamp),
    )
