from __future__ import annotations

import random
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Any


def utcnow() -> datetime:
    return datetime.now(tz=UTC)


class TaskState(str, Enum):
    QUEUED = "queued"
    ADMITTED = "admitted"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class NodeMode(str, Enum):
    ACTIVE = "active"
    DRAINING = "draining"
    SHUTTING_DOWN = "shutting_down"


class HealthState(str, Enum):
    LIVE = "live"
    READY = "ready"
    DEGRADED = "degraded"
    NOT_READY = "not_ready"


class OperationType(str, Enum):
    GRAYSCALE = "grayscale"
    RESIZE = "resize"
    CROP = "crop"
    ROTATE = "rotate"
    FLIP = "flip"
    BLUR = "blur"
    SHARPEN = "sharpen"
    BRIGHTNESS_CONTRAST = "brightness_contrast"
    WATERMARK_TEXT = "watermark_text"
    FORMAT_CONVERSION = "format_conversion"
    OCR = "ocr"
    INFERENCE = "inference"


class ExecutorKind(str, Enum):
    THREAD = "thread"
    PROCESS = "process"
    DEDICATED_PROCESS = "dedicated_process"


@dataclass(slots=True)
class TransformationSpec:
    operation: OperationType
    params: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class InputImageRef:
    image_id: str
    input_path: str | None = None
    input_uri: str | None = None
    payload: bytes | None = None
    image_format: str | None = None
    size_bytes: int = 0
    width: int = 0
    height: int = 0


@dataclass(slots=True)
class Task:
    task_id: str
    idempotency_key: str
    priority: int
    created_at: datetime
    deadline: datetime | None
    max_retries: int
    transforms: list[TransformationSpec]
    input_image: InputImageRef
    output_format: str | None
    metadata: dict[str, str] = field(default_factory=dict)
    attempt: int = 0
    status: TaskState = TaskState.QUEUED
    estimated_service_ms: float = 0.0
    estimated_cost: float = 0.0
    last_error: str | None = None
    enqueued_at_monotonic: float = field(default_factory=time.monotonic)
    next_eligible_at_monotonic: float = 0.0
    cancel_token_path: str | None = None


@dataclass(slots=True)
class ResourceRequirements:
    cpu_units: float
    memory_bytes: int
    io_units: float
    gpu_units: float = 0.0


@dataclass(slots=True)
class ResourceSnapshot:
    cpu_utilization: float
    memory_utilization: float
    gpu_utilization: float
    io_utilization: float
    available_memory_bytes: int
    reserved_memory_bytes: int
    queue_length: int
    active_tasks: int
    capacity_effective: int
    captured_at: datetime = field(default_factory=utcnow)


@dataclass(order=True, slots=True)
class PriorityQueueItem:
    sort_index: tuple[float, float, int]
    task_id: str = field(compare=False)
    version: int = field(compare=False)
    score: float = field(compare=False)


@dataclass(slots=True)
class RetryPolicy:
    base_delay_ms: int
    max_delay_ms: int
    jitter_ratio: float = 0.2

    def next_delay_seconds(self, attempt_number: int) -> float:
        raw = min(self.base_delay_ms * (2 ** max(0, attempt_number - 1)), self.max_delay_ms)
        jitter = raw * self.jitter_ratio * random.random()
        return (raw + jitter) / 1000.0


@dataclass(slots=True)
class ProgressEventRecord:
    task_id: str
    image_id: str
    node_id: str
    state: TaskState
    progress_pct: int
    attempt: int
    queue_wait_ms: int
    run_time_ms: int
    message: str
    metadata: dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class ExecutionResultRecord:
    task_id: str
    image_id: str
    node_id: str
    state: TaskState
    attempt: int
    output_path: str | None
    output_format: str | None
    width: int
    height: int
    size_bytes: int
    error_code: str | None = None
    error_message: str | None = None
    metadata: dict[str, str] = field(default_factory=dict)
    started_at: datetime | None = None
    finished_at: datetime | None = None


@dataclass(slots=True)
class NodeState:
    node_id: str
    mode: NodeMode
    accepting_tasks: bool
    queue_length: int
    active_tasks: int
    capacity_effective: int
    cpu_utilization: float
    memory_utilization: float
    gpu_utilization: float
    io_utilization: float
    available_memory_bytes: int
    reserved_memory_bytes: int
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class NodeHealth:
    node_id: str
    state: HealthState
    live: bool
    ready: bool
    coordinator_connected: bool
    queue_length: int
    active_tasks: int
    message: str
    timestamp: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class PersistedTaskRecord:
    task_id: str
    idempotency_key: str
    image_id: str
    state: TaskState
    finished_at: datetime
    output_path: str | None
    output_format: str | None
    error_code: str | None = None
    error_message: str | None = None


@dataclass(slots=True)
class SpoolEventRecord:
    spool_id: str
    kind: str
    payload: Any


@dataclass(slots=True)
class LogEntry:
    level: str
    message: str
    task_id: str | None
    image_id: str | None
    node_id: str
    attempt: int | None
    state: str | None
    latency_ms: int | None
    extra: dict[str, Any] = field(default_factory=dict)
