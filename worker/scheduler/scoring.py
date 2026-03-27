from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from worker.config import WorkerConfig
from worker.models.types import Task


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


@dataclass(slots=True)
class ScoreWeights:
    priority: float = 0.35
    deadline: float = 0.25
    aging: float = 0.20
    cost_penalty: float = 0.10
    retry_penalty: float = 0.05
    size_penalty: float = 0.05


class TaskScorer:
    def __init__(self, config: WorkerConfig, weights: ScoreWeights | None = None) -> None:
        self._config = config
        self._weights = weights or ScoreWeights()

    def compute_score(self, task: Task, now_monotonic: float) -> float:
        priority_term = _clamp(task.priority / 10.0)
        deadline_term = self._deadline_urgency(task)
        aging_term = self._aging(task, now_monotonic)
        cost_term = _clamp(task.estimated_service_ms / max(self._config.score_cost_window_ms, 1.0))
        retry_term = _clamp(task.attempt / max(task.max_retries, 1))
        size_term = _clamp(task.input_image.size_bytes / max(self._config.large_image_threshold_bytes, 1))
        return (
            self._weights.priority * priority_term
            + self._weights.deadline * deadline_term
            + self._weights.aging * aging_term
            - self._weights.cost_penalty * cost_term
            - self._weights.retry_penalty * retry_term
            - self._weights.size_penalty * size_term
        )

    def deadline_sort_key(self, task: Task) -> float:
        if task.deadline is None:
            return float("inf")
        return task.deadline.timestamp()

    def _deadline_urgency(self, task: Task) -> float:
        if task.deadline is None:
            return 0.15
        now = datetime.now(tz=UTC)
        seconds_left = (task.deadline - now).total_seconds()
        if seconds_left <= 0:
            return 1.0
        return _clamp(1.0 - (seconds_left / self._config.score_deadline_window_seconds))

    def _aging(self, task: Task, now_monotonic: float) -> float:
        waited = max(0.0, now_monotonic - task.enqueued_at_monotonic)
        return _clamp(waited / max(self._config.score_aging_window_seconds, 0.1))
