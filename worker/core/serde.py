from __future__ import annotations

import base64
from datetime import UTC, datetime
from typing import Any

from worker.models.types import (
    ExecutionResultRecord,
    InputImageRef,
    OperationType,
    ProgressEventRecord,
    Task,
    TaskState,
    TransformationSpec,
)


def _dt_to_str(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _str_to_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value).astimezone(UTC)


def _bytes_to_str(value: bytes | None) -> str | None:
    if value is None:
        return None
    return base64.b64encode(value).decode("ascii")


def _str_to_bytes(value: str | None) -> bytes | None:
    if value is None:
        return None
    return base64.b64decode(value.encode("ascii"))


def task_to_dict(task: Task) -> dict[str, Any]:
    return {
        "task_id": task.task_id,
        "idempotency_key": task.idempotency_key,
        "priority": task.priority,
        "created_at": _dt_to_str(task.created_at),
        "deadline": _dt_to_str(task.deadline),
        "max_retries": task.max_retries,
        "transforms": [
            {"operation": item.operation.value, "params": dict(item.params)}
            for item in task.transforms
        ],
        "input_image": {
            "image_id": task.input_image.image_id,
            "input_path": task.input_image.input_path,
            "input_uri": task.input_image.input_uri,
            "payload": _bytes_to_str(task.input_image.payload),
            "image_format": task.input_image.image_format,
            "size_bytes": task.input_image.size_bytes,
            "width": task.input_image.width,
            "height": task.input_image.height,
        },
        "output_format": task.output_format,
        "metadata": dict(task.metadata),
        "attempt": task.attempt,
        "status": task.status.value,
        "estimated_service_ms": task.estimated_service_ms,
        "estimated_cost": task.estimated_cost,
        "last_error": task.last_error,
        "next_eligible_at_monotonic": task.next_eligible_at_monotonic,
        "cancel_token_path": task.cancel_token_path,
    }


def task_from_dict(payload: dict[str, Any]) -> Task:
    image = payload["input_image"]
    input_image_payload: dict[str, Any] = {
        "image_id": image["image_id"],
        "input_path": image.get("input_path"),
        "input_uri": image.get("input_uri"),
        "payload": _str_to_bytes(image.get("payload")),
        "image_format": image.get("image_format"),
        "size_bytes": int(image.get("size_bytes", 0)),
        "width": int(image.get("width", 0)),
        "height": int(image.get("height", 0)),
    }
    task_payload: dict[str, Any] = {
        "task_id": payload["task_id"],
        "idempotency_key": payload["idempotency_key"],
        "priority": int(payload["priority"]),
        "created_at": _str_to_dt(payload["created_at"]) or datetime.now(tz=UTC),
        "deadline": _str_to_dt(payload.get("deadline")),
        "max_retries": int(payload["max_retries"]),
        "transforms": [
            TransformationSpec(operation=OperationType(item["operation"]), params=dict(item.get("params", {})))
            for item in payload.get("transforms", [])
        ],
        "input_image": InputImageRef(**input_image_payload),
        "output_format": payload.get("output_format"),
        "metadata": dict(payload.get("metadata", {})),
        "attempt": int(payload.get("attempt", 0)),
        "status": TaskState(payload.get("status", TaskState.QUEUED.value)),
        "estimated_service_ms": float(payload.get("estimated_service_ms", 0.0)),
        "estimated_cost": float(payload.get("estimated_cost", 0.0)),
        "last_error": payload.get("last_error"),
        "next_eligible_at_monotonic": float(payload.get("next_eligible_at_monotonic", 0.0)),
        "cancel_token_path": payload.get("cancel_token_path"),
    }
    return Task(**task_payload)


def progress_to_dict(event: ProgressEventRecord) -> dict[str, Any]:
    return {
        "task_id": event.task_id,
        "image_id": event.image_id,
        "node_id": event.node_id,
        "state": event.state.value,
        "progress_pct": event.progress_pct,
        "attempt": event.attempt,
        "queue_wait_ms": event.queue_wait_ms,
        "run_time_ms": event.run_time_ms,
        "message": event.message,
        "metadata": dict(event.metadata),
        "timestamp": _dt_to_str(event.timestamp),
    }


def progress_from_dict(payload: dict[str, Any]) -> ProgressEventRecord:
    progress_payload: dict[str, Any] = {
        "task_id": payload["task_id"],
        "image_id": payload["image_id"],
        "node_id": payload["node_id"],
        "state": TaskState(payload["state"]),
        "progress_pct": int(payload["progress_pct"]),
        "attempt": int(payload["attempt"]),
        "queue_wait_ms": int(payload["queue_wait_ms"]),
        "run_time_ms": int(payload["run_time_ms"]),
        "message": payload["message"],
        "metadata": dict(payload.get("metadata", {})),
        "timestamp": _str_to_dt(payload.get("timestamp")) or datetime.now(tz=UTC),
    }
    return ProgressEventRecord(**progress_payload)


def result_to_dict(result: ExecutionResultRecord) -> dict[str, Any]:
    return {
        "task_id": result.task_id,
        "image_id": result.image_id,
        "node_id": result.node_id,
        "state": result.state.value,
        "attempt": result.attempt,
        "output_path": result.output_path,
        "output_format": result.output_format,
        "width": result.width,
        "height": result.height,
        "size_bytes": result.size_bytes,
        "error_code": result.error_code,
        "error_message": result.error_message,
        "metadata": dict(result.metadata),
        "started_at": _dt_to_str(result.started_at),
        "finished_at": _dt_to_str(result.finished_at),
    }


def result_from_dict(payload: dict[str, Any]) -> ExecutionResultRecord:
    result_payload: dict[str, Any] = {
        "task_id": payload["task_id"],
        "image_id": payload["image_id"],
        "node_id": payload["node_id"],
        "state": TaskState(payload["state"]),
        "attempt": int(payload["attempt"]),
        "output_path": payload.get("output_path"),
        "output_format": payload.get("output_format"),
        "width": int(payload.get("width", 0)),
        "height": int(payload.get("height", 0)),
        "size_bytes": int(payload.get("size_bytes", 0)),
        "error_code": payload.get("error_code"),
        "error_message": payload.get("error_message"),
        "metadata": dict(payload.get("metadata", {})),
        "started_at": _str_to_dt(payload.get("started_at")),
        "finished_at": _str_to_dt(payload.get("finished_at")),
    }
    return ExecutionResultRecord(**result_payload)
