from __future__ import annotations

from datetime import UTC, datetime

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


TASK_STATUS_TO_PROTO = {
    TaskState.QUEUED: worker_node_pb2.TASK_STATUS_QUEUED,
    TaskState.ADMITTED: worker_node_pb2.TASK_STATUS_ADMITTED,
    TaskState.RUNNING: worker_node_pb2.TASK_STATUS_RUNNING,
    TaskState.RETRY_SCHEDULED: worker_node_pb2.TASK_STATUS_RETRY_SCHEDULED,
    TaskState.SUCCEEDED: worker_node_pb2.TASK_STATUS_SUCCEEDED,
    TaskState.FAILED: worker_node_pb2.TASK_STATUS_FAILED,
    TaskState.CANCELLED: worker_node_pb2.TASK_STATUS_CANCELLED,
    TaskState.REJECTED: worker_node_pb2.TASK_STATUS_REJECTED,
}

NODE_MODE_TO_PROTO = {
    NodeMode.ACTIVE: worker_node_pb2.NODE_MODE_ACTIVE,
    NodeMode.DRAINING: worker_node_pb2.NODE_MODE_DRAINING,
    NodeMode.SHUTTING_DOWN: worker_node_pb2.NODE_MODE_SHUTTING_DOWN,
}

IMAGE_FORMAT_FROM_PROTO = {
    worker_node_pb2.IMAGE_FORMAT_JPG: "jpg",
    worker_node_pb2.IMAGE_FORMAT_PNG: "png",
    worker_node_pb2.IMAGE_FORMAT_TIF: "tif",
}

IMAGE_FORMAT_TO_PROTO = {
    "jpg": worker_node_pb2.IMAGE_FORMAT_JPG,
    "jpeg": worker_node_pb2.IMAGE_FORMAT_JPG,
    "png": worker_node_pb2.IMAGE_FORMAT_PNG,
    "tif": worker_node_pb2.IMAGE_FORMAT_TIF,
    "tiff": worker_node_pb2.IMAGE_FORMAT_TIF,
}

OPERATION_FROM_PROTO = {
    worker_node_pb2.OPERATION_GRAYSCALE: OperationType.GRAYSCALE,
    worker_node_pb2.OPERATION_RESIZE: OperationType.RESIZE,
    worker_node_pb2.OPERATION_CROP: OperationType.CROP,
    worker_node_pb2.OPERATION_ROTATE: OperationType.ROTATE,
    worker_node_pb2.OPERATION_FLIP: OperationType.FLIP,
    worker_node_pb2.OPERATION_BLUR: OperationType.BLUR,
    worker_node_pb2.OPERATION_SHARPEN: OperationType.SHARPEN,
    worker_node_pb2.OPERATION_BRIGHTNESS_CONTRAST: OperationType.BRIGHTNESS_CONTRAST,
    worker_node_pb2.OPERATION_WATERMARK_TEXT: OperationType.WATERMARK_TEXT,
    worker_node_pb2.OPERATION_FORMAT_CONVERSION: OperationType.FORMAT_CONVERSION,
    worker_node_pb2.OPERATION_OCR: OperationType.OCR,
    worker_node_pb2.OPERATION_INFERENCE: OperationType.INFERENCE,
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


def task_from_proto(message: worker_node_pb2.Task) -> Task:
    source = message.input.WhichOneof("source")
    image = InputImageRef(
        image_id=message.input.image_id,
        input_path=message.input.input_path if source == "input_path" else None,
        input_uri=message.input.input_uri if source == "input_uri" else None,
        payload=message.input.content if source == "content" else None,
        image_format=IMAGE_FORMAT_FROM_PROTO.get(message.input.format, "png"),
        size_bytes=message.input.size_bytes,
        width=message.input.width,
        height=message.input.height,
    )
    return Task(
        task_id=message.task_id,
        idempotency_key=message.idempotency_key or message.task_id,
        priority=message.priority,
        created_at=datetime_from_timestamp(message.created_at) or datetime.now(tz=UTC),
        deadline=datetime_from_timestamp(message.deadline),
        max_retries=message.max_retries,
        transforms=[
            TransformationSpec(operation=OPERATION_FROM_PROTO[item.type], params=dict(item.params))
            for item in message.transforms
        ],
        input_image=image,
        output_format=IMAGE_FORMAT_FROM_PROTO.get(message.output_format, image.image_format or "png"),
        metadata=dict(message.metadata),
    )


def progress_to_proto(event: ProgressEventRecord) -> worker_node_pb2.ProgressEvent:
    return worker_node_pb2.ProgressEvent(
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


def result_to_proto(result: ExecutionResultRecord) -> worker_node_pb2.ExecutionResult:
    return worker_node_pb2.ExecutionResult(
        task_id=result.task_id,
        image_id=result.image_id,
        node_id=result.node_id,
        status=TASK_STATUS_TO_PROTO[result.state],
        attempt=result.attempt,
        output_path=result.output_path or "",
        output_format=IMAGE_FORMAT_TO_PROTO.get((result.output_format or "png").lower(), worker_node_pb2.IMAGE_FORMAT_PNG),
        width=result.width,
        height=result.height,
        size_bytes=result.size_bytes,
        error_code=result.error_code or "",
        error_message=result.error_message or "",
        started_at=timestamp_from_datetime(result.started_at),
        finished_at=timestamp_from_datetime(result.finished_at),
        metadata=strip_internal_trace_metadata(result.metadata),
    )


def node_status_to_proto(state: NodeState) -> worker_node_pb2.NodeStatus:
    return worker_node_pb2.NodeStatus(
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
