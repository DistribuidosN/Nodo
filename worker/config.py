from __future__ import annotations

import os
import socket
from dataclasses import dataclass
from pathlib import Path


def _get_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return int(value) if value is not None else default


def _get_float(name: str, default: float) -> float:
    value = os.getenv(name)
    return float(value) if value is not None else default


@dataclass(slots=True)
class WorkerConfig:
    node_id: str
    bind_host: str
    bind_port: int
    coordinator_target: str
    metrics_host: str
    metrics_port: int
    health_host: str
    health_port: int
    output_dir: Path
    state_dir: Path
    max_active_tasks: int
    process_pool_workers: int
    thread_pool_workers: int
    cpu_target: float
    max_queue_size: int
    queue_high_watermark: int
    queue_low_watermark: int
    min_free_memory_bytes: int
    large_image_threshold_bytes: int
    heartbeat_interval_seconds: float
    report_queue_size: int
    retry_base_ms: int
    retry_max_ms: int
    score_deadline_window_seconds: float
    score_aging_window_seconds: float
    score_cost_window_ms: float
    scheduler_poll_seconds: float
    dedupe_ttl_seconds: int
    coordinator_reconnect_base_seconds: float
    coordinator_reconnect_max_seconds: float
    coordinator_failure_threshold: int
    graceful_shutdown_timeout_seconds: float
    process_cancel_grace_seconds: float
    process_kill_timeout_seconds: float
    log_level: str

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        cpu_count = os.cpu_count() or 4
        max_active = _get_int("WORKER_MAX_ACTIVE_TASKS", max(2, cpu_count))
        process_workers = _get_int("WORKER_PROCESS_POOL", max(1, cpu_count - 1))
        high_watermark = _get_int("WORKER_QUEUE_HWM", max(8, int(max_active * 4)))
        low_watermark = _get_int("WORKER_QUEUE_LWM", max(4, int(high_watermark * 0.66)))
        return cls(
            node_id=os.getenv("WORKER_NODE_ID", socket.gethostname()),
            bind_host=os.getenv("WORKER_BIND_HOST", "127.0.0.1"),
            bind_port=_get_int("WORKER_BIND_PORT", 50051),
            coordinator_target=os.getenv("WORKER_COORDINATOR_TARGET", "127.0.0.1:50052"),
            metrics_host=os.getenv("WORKER_METRICS_HOST", "127.0.0.1"),
            metrics_port=_get_int("WORKER_METRICS_PORT", 9100),
            health_host=os.getenv("WORKER_HEALTH_HOST", "127.0.0.1"),
            health_port=_get_int("WORKER_HEALTH_PORT", 8081),
            output_dir=Path(os.getenv("WORKER_OUTPUT_DIR", "data/out")),
            state_dir=Path(os.getenv("WORKER_STATE_DIR", "data/state")),
            max_active_tasks=max_active,
            process_pool_workers=process_workers,
            thread_pool_workers=_get_int("WORKER_THREAD_POOL", min(8, max_active)),
            cpu_target=_get_float("WORKER_CPU_TARGET", 0.85),
            max_queue_size=_get_int("WORKER_MAX_QUEUE_SIZE", max(high_watermark + 16, max_active * 6)),
            queue_high_watermark=high_watermark,
            queue_low_watermark=low_watermark,
            min_free_memory_bytes=_get_int("WORKER_MIN_FREE_MEMORY_BYTES", 256 * 1024 * 1024),
            large_image_threshold_bytes=_get_int("WORKER_LARGE_IMAGE_THRESHOLD_BYTES", 32 * 1024 * 1024),
            heartbeat_interval_seconds=_get_float("WORKER_HEARTBEAT_INTERVAL_SECONDS", 5.0),
            report_queue_size=_get_int("WORKER_REPORT_QUEUE_SIZE", 512),
            retry_base_ms=_get_int("WORKER_RETRY_BASE_MS", 500),
            retry_max_ms=_get_int("WORKER_RETRY_MAX_MS", 15_000),
            score_deadline_window_seconds=_get_float("WORKER_SCORE_DEADLINE_WINDOW_SECONDS", 30.0),
            score_aging_window_seconds=_get_float("WORKER_SCORE_AGING_WINDOW_SECONDS", 60.0),
            score_cost_window_ms=_get_float("WORKER_SCORE_COST_WINDOW_MS", 1500.0),
            scheduler_poll_seconds=_get_float("WORKER_SCHEDULER_POLL_SECONDS", 0.05),
            dedupe_ttl_seconds=_get_int("WORKER_DEDUPE_TTL_SECONDS", 3600),
            coordinator_reconnect_base_seconds=_get_float("WORKER_COORDINATOR_RECONNECT_BASE_SECONDS", 1.0),
            coordinator_reconnect_max_seconds=_get_float("WORKER_COORDINATOR_RECONNECT_MAX_SECONDS", 15.0),
            coordinator_failure_threshold=_get_int("WORKER_COORDINATOR_FAILURE_THRESHOLD", 3),
            graceful_shutdown_timeout_seconds=_get_float("WORKER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS", 10.0),
            process_cancel_grace_seconds=_get_float("WORKER_PROCESS_CANCEL_GRACE_SECONDS", 0.75),
            process_kill_timeout_seconds=_get_float("WORKER_PROCESS_KILL_TIMEOUT_SECONDS", 2.0),
            log_level=os.getenv("WORKER_LOG_LEVEL", "INFO"),
        )

    @property
    def bind_target(self) -> str:
        return f"{self.bind_host}:{self.bind_port}"
