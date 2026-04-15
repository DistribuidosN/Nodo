"""
Estado del nodo (asyncio-safe, sin locks de threading).

Un proceso = un worker = un event loop → no hay race conditions en estado simple.
El único acceso concurrente real es desde run_in_executor (proceso hijo),
pero ese solo devuelve el resultado, no toca NodeState directamente.
"""
from __future__ import annotations

import statistics
import time

import psutil

from config.settings import NODE_ID, NODE_IP, QUEUE_CAPACITY, get_logger

log = get_logger(NODE_ID)


class NodeState:
    """Estado mutable del worker. Solo accedido desde el event loop principal."""

    def __init__(self) -> None:
        self.tasks_done     = 0
        self.tasks_error    = 0
        self.steals_done    = 0
        self.latencies_ms: list[float] = []   # últimas 200 latencias
        self.is_busy        = False
        self.start_time     = time.monotonic()
        self.current_status = "IDLE"          # IDLE | BUSY | STEALING | ERROR

    # ── Registro ──────────────────────────────────────────────────────────────

    def record_done(self, latency_ms: float, error: bool = False) -> None:
        if error:
            self.tasks_error += 1
        else:
            self.tasks_done += 1
        self.latencies_ms.append(latency_ms)
        if len(self.latencies_ms) > 200:
            self.latencies_ms.pop(0)

    def record_steal(self, n: int = 1) -> None:
        self.steals_done += n

    def set_busy(self, busy: bool) -> None:
        self.is_busy        = busy
        self.current_status = "BUSY" if busy else "IDLE"

    def set_status(self, s: str) -> None:
        self.current_status = s

    # ── Snapshot (devuelve dict plano, sin dependencia de protobuf) ───────────

    def metrics_dict(self, queue_size: int) -> dict:
        mem     = psutil.virtual_memory()
        cpu_pct = psutil.cpu_percent(interval=None)
        lats    = self.latencies_ms
        avg     = statistics.mean(lats) if lats else 0.0
        p95     = (
            statistics.quantiles(lats, n=20)[18]
            if len(lats) >= 20
            else (max(lats) if lats else 0.0)
        )
        return {
            "node_id":          NODE_ID,
            "ip_address":       NODE_IP,
            "ram_used_mb":      mem.used  / 1024 ** 2,
            "ram_total_mb":     mem.total / 1024 ** 2,
            "cpu_percent":      cpu_pct,
            "workers_busy":     1 if self.is_busy else 0,
            "workers_total":    1,
            "queue_size":       queue_size,
            "queue_capacity":   QUEUE_CAPACITY,
            "tasks_done":       self.tasks_done,
            "steals_performed": self.steals_done,
            "avg_latency_ms":   avg,
            "p95_latency_ms":   p95,
            "uptime_seconds":   int(time.monotonic() - self.start_time),
            "status":           self.current_status,
        }
