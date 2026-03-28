from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass


_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


@dataclass(slots=True)
class LeadershipState:
    leader: bool
    holder: str | None


class CoordinatorLeaderElector:
    def __init__(self, *, redis_url: str | None, cluster_id: str, instance_id: str, lock_ttl_seconds: float) -> None:
        self._logger = logging.getLogger("coordinator.leadership")
        self._redis_url = redis_url
        self._cluster_id = cluster_id
        self._instance_id = instance_id
        self._ttl_ms = max(1_000, int(lock_ttl_seconds * 1000))
        self._lock_key = f"coordinator:{cluster_id}:leader"
        self._redis = None
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._is_leader = redis_url is None
        self._holder: str | None = instance_id if redis_url is None else None

    @property
    def is_leader(self) -> bool:
        return self._is_leader

    @property
    def holder(self) -> str | None:
        return self._holder

    async def start(self) -> None:
        if self._redis_url is None:
            return
        from redis import asyncio as redis_asyncio

        self._redis = redis_asyncio.from_url(self._redis_url, encoding="utf-8", decode_responses=True)
        self._task = asyncio.create_task(self._loop(), name="coordinator-leader-election")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        if self._redis is not None:
            if self._is_leader:
                with contextlib.suppress(Exception):
                    await self._redis.eval(_RELEASE_SCRIPT, 1, self._lock_key, self._instance_id)
            await self._redis.close()

    async def _loop(self) -> None:
        assert self._redis is not None
        sleep_seconds = max(1.0, self._ttl_ms / 3000.0)
        while not self._stop_event.is_set():
            try:
                if self._is_leader:
                    renewed = await self._redis.eval(_RENEW_SCRIPT, 1, self._lock_key, self._instance_id, str(self._ttl_ms))
                    if int(renewed or 0) != 1:
                        self._is_leader = False
                else:
                    acquired = await self._redis.set(self._lock_key, self._instance_id, nx=True, px=self._ttl_ms)
                    self._is_leader = bool(acquired)
                holder = await self._redis.get(self._lock_key)
                self._holder = holder
            except Exception as exc:  # pragma: no cover
                self._logger.warning("leader election check failed", extra={"extra_data": {"error": str(exc)}})
                self._is_leader = False
                self._holder = None
            await asyncio.sleep(sleep_seconds)
