from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections import Counter

from worker.domain.models import PriorityQueueItem, Task
from worker.scheduler.scoring import TaskScorer


class TaskPriorityQueue:
    def __init__(self, scorer: TaskScorer) -> None:
        self._scorer = scorer
        self._heap: list[PriorityQueueItem] = []
        self._entries: dict[str, Task] = {}
        self._versions: dict[str, int] = {}
        self._sequence = itertools.count()
        self._lock = asyncio.Lock()
        self._not_empty = asyncio.Event()

    async def enqueue(self, task: Task) -> int:
        async with self._lock:
            version = self._versions.get(task.task_id, 0) + 1
            self._versions[task.task_id] = version
            score = self._scorer.compute_score(task, time.monotonic())
            item = PriorityQueueItem(
                sort_index=(-score, self._scorer.deadline_sort_key(task), next(self._sequence)),
                task_id=task.task_id,
                version=version,
                score=score,
            )
            self._entries[task.task_id] = task
            heapq.heappush(self._heap, item)
            self._not_empty.set()
            return len(self._entries)

    async def remove(self, task_id: str) -> Task | None:
        async with self._lock:
            task = self._entries.pop(task_id, None)
            if task is not None:
                self._versions[task_id] = self._versions.get(task_id, 0) + 1
            if not self._entries:
                self._not_empty.clear()
            return task

    async def pop_ready(self) -> tuple[Task | None, float | None]:
        deferred: list[PriorityQueueItem] = []
        selected: Task | None = None
        next_wait: float | None = None
        now = time.monotonic()

        async with self._lock:
            while self._heap:
                item = heapq.heappop(self._heap)
                task = self._entries.get(item.task_id)
                version = self._versions.get(item.task_id)
                if task is None or version != item.version:
                    continue

                if task.next_eligible_at_monotonic > now:
                    wait_for = task.next_eligible_at_monotonic - now
                    next_wait = wait_for if next_wait is None else min(next_wait, wait_for)
                    deferred.append(item)
                    continue

                new_score = self._scorer.compute_score(task, now)
                if abs(new_score - item.score) > 0.05:
                    version = self._versions.get(task.task_id, 0) + 1
                    self._versions[task.task_id] = version
                    deferred.append(
                        PriorityQueueItem(
                            sort_index=(-new_score, self._scorer.deadline_sort_key(task), next(self._sequence)),
                            task_id=task.task_id,
                            version=version,
                            score=new_score,
                        )
                    )
                    continue

                selected = self._entries.pop(task.task_id)
                break

            for entry in deferred:
                heapq.heappush(self._heap, entry)
            if not self._entries:
                self._not_empty.clear()

        return selected, next_wait

    async def wait_for_items(self, timeout: float | None = None) -> None:
        try:
            await asyncio.wait_for(self._not_empty.wait(), timeout=timeout)
        except TimeoutError:
            return

    async def task_counts_by_priority(self) -> dict[int, int]:
        async with self._lock:
            counts = Counter(task.priority for task in self._entries.values())
            return dict(counts)

    async def steal(self, count: int) -> list[Task]:
        if count <= 0:
            return []

        now = time.monotonic()
        async with self._lock:
            ranked = sorted(
                (
                    (
                        self._scorer.compute_score(task, now),
                        self._scorer.deadline_sort_key(task),
                        task.task_id,
                    )
                    for task in self._entries.values()
                ),
                key=lambda item: (item[0], item[1], item[2]),
            )
            stolen: list[Task] = []
            for _, _, task_id in ranked[:count]:
                task = self._entries.pop(task_id, None)
                if task is None:
                    continue
                self._versions[task_id] = self._versions.get(task_id, 0) + 1
                stolen.append(task)
            if not self._entries:
                self._not_empty.clear()
            return stolen

    async def qsize(self) -> int:
        async with self._lock:
            return len(self._entries)
