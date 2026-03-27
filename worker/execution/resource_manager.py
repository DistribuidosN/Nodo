from __future__ import annotations

import asyncio
import math

import psutil

from worker.config import WorkerConfig
from worker.models.types import ResourceRequirements, ResourceSnapshot, Task


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


class ResourceManager:
    def __init__(self, config: WorkerConfig) -> None:
        self._config = config
        self._reservations: dict[str, ResourceRequirements] = {}
        self._lock = asyncio.Lock()
        psutil.cpu_percent(interval=None)

    async def estimate_requirements(self, task: Task) -> ResourceRequirements:
        size_bytes = max(task.input_image.size_bytes, task.input_image.width * task.input_image.height * 3)
        memory_bytes = max(int(size_bytes * 2.5), 32 * 1024 * 1024)
        cpu_units = min(max(task.estimated_service_ms / 250.0, 0.25), 2.0)
        io_units = min(max(size_bytes / (32 * 1024 * 1024), 0.1), 1.0)
        return ResourceRequirements(cpu_units=cpu_units, memory_bytes=memory_bytes, io_units=io_units)

    async def snapshot(self, queue_length: int, active_tasks: int) -> ResourceSnapshot:
        memory = psutil.virtual_memory()
        cpu_utilization = psutil.cpu_percent(interval=None) / 100.0
        reserved_memory = sum(item.memory_bytes for item in self._reservations.values())
        reserved_io = sum(item.io_units for item in self._reservations.values())
        cpu_factor = _clamp((self._config.cpu_target - cpu_utilization) / max(self._config.cpu_target, 0.01))
        memory_factor = _clamp(
            (memory.available - self._config.min_free_memory_bytes - reserved_memory)
            / max(memory.total - self._config.min_free_memory_bytes, 1)
        )
        io_factor = _clamp(1.0 - (reserved_io / max(self._config.max_active_tasks, 1)))
        capacity = max(0, math.floor(min(cpu_factor, memory_factor, io_factor, 1.0) * self._config.max_active_tasks))
        return ResourceSnapshot(
            cpu_utilization=cpu_utilization,
            memory_utilization=memory.percent / 100.0,
            gpu_utilization=0.0,
            io_utilization=1.0 - io_factor,
            available_memory_bytes=memory.available,
            reserved_memory_bytes=reserved_memory,
            queue_length=queue_length,
            active_tasks=active_tasks,
            capacity_effective=capacity,
        )

    async def can_run(self, task: Task, queue_length: int, active_tasks: int) -> tuple[bool, ResourceRequirements, ResourceSnapshot]:
        requirements = await self.estimate_requirements(task)
        snapshot = await self.snapshot(queue_length=queue_length, active_tasks=active_tasks)
        available_after_reserve = snapshot.available_memory_bytes - snapshot.reserved_memory_bytes
        enough_memory = available_after_reserve >= requirements.memory_bytes + self._config.min_free_memory_bytes
        enough_slots = active_tasks < max(snapshot.capacity_effective, 1)
        return enough_memory and enough_slots, requirements, snapshot

    async def reserve(self, task_id: str, requirements: ResourceRequirements) -> None:
        async with self._lock:
            self._reservations[task_id] = requirements

    async def release(self, task_id: str) -> None:
        async with self._lock:
            self._reservations.pop(task_id, None)
