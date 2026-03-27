from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable, Callable

import grpc

from proto import worker_node_pb2, worker_node_pb2_grpc
from worker.config import WorkerConfig
from worker.core.state_store import EventSpoolStore
from worker.grpc.mappers import node_status_to_proto, progress_to_proto, result_to_proto
from worker.models.types import ExecutionResultRecord, NodeState, ProgressEventRecord
from worker.telemetry.metrics import WorkerMetrics


StatusProvider = Callable[[], Awaitable[NodeState]]


class CoordinatorReporter:
    def __init__(self, config: WorkerConfig, metrics: WorkerMetrics, status_provider: StatusProvider) -> None:
        self._config = config
        self._metrics = metrics
        self._status_provider = status_provider
        self._logger = logging.getLogger("worker.reporter")
        self._queue: asyncio.Queue[tuple[str, str, ProgressEventRecord | ExecutionResultRecord]] = asyncio.Queue(
            maxsize=config.report_queue_size
        )
        self._stop_event = asyncio.Event()
        self._channel: grpc.aio.Channel | None = None
        self._stub: worker_node_pb2_grpc.CoordinatorCallbackServiceStub | None = None
        self._tasks: list[asyncio.Task] = []
        self._failure_count = 0
        self._connected = False
        self._channel_lock = asyncio.Lock()
        self._spool = EventSpoolStore(config.state_dir)

    async def start(self) -> None:
        await self._connect()
        for item in self._spool.load_pending():
            await self._queue.put((item.spool_id, item.kind, item.payload))
        self._metrics.set_report_queue_length(self._queue.qsize())
        self._tasks = [
            asyncio.create_task(self._report_loop(), name="coordinator-report-loop"),
            asyncio.create_task(self._heartbeat_loop(), name="coordinator-heartbeat-loop"),
        ]

    async def stop(self) -> None:
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._queue.join(), timeout=2.0)
        except TimeoutError:
            pass
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError):
                await task
        if self._channel is not None:
            await self._channel.close()
        self._metrics.set_coordinator_connected(False)

    async def report_progress(self, event: ProgressEventRecord) -> None:
        event.metadata["spool_kind"] = "progress"
        spool_id = self._spool.append("progress", event)
        event.metadata["spool_id"] = spool_id
        await self._queue.put((spool_id, "progress", event))
        self._metrics.set_report_queue_length(self._queue.qsize())

    async def report_result(self, result: ExecutionResultRecord) -> None:
        result.metadata["spool_kind"] = "result"
        spool_id = self._spool.append("result", result)
        result.metadata["spool_id"] = spool_id
        await self._queue.put((spool_id, "result", result))
        self._metrics.set_report_queue_length(self._queue.qsize())

    @property
    def connected(self) -> bool:
        return self._connected

    async def _report_loop(self) -> None:
        while not self._stop_event.is_set() or not self._queue.empty():
            spool_id, kind, payload = await self._queue.get()
            try:
                await self._send_event(kind, payload)
                self._spool.ack(spool_id)
            except Exception as exc:  # pragma: no cover
                self._logger.warning("coordinator event delivery failed", extra={"extra_data": {"error": str(exc)}})
                await self._mark_failure()
                await asyncio.sleep(self._retry_delay())
                await self._queue.put((spool_id, kind, payload))
            finally:
                self._metrics.set_report_queue_length(self._queue.qsize())
                self._queue.task_done()

    async def _send_event(self, kind: str, payload: ProgressEventRecord | ExecutionResultRecord) -> None:
        stub = await self._ensure_stub()
        started = time.perf_counter()
        if kind == "progress":
            await stub.ReportProgress(progress_to_proto(payload), wait_for_ready=True)
        else:
            await stub.ReportResult(result_to_proto(payload), wait_for_ready=True)
        self._metrics.grpc_rtt_ms.observe((time.perf_counter() - started) * 1000)
        await self._mark_success()

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                status = await self._status_provider()
                stub = await self._ensure_stub()
                started = time.perf_counter()
                await stub.Heartbeat(
                    worker_node_pb2.HeartbeatRequest(status=node_status_to_proto(status)),
                    wait_for_ready=True,
                )
                self._metrics.grpc_rtt_ms.observe((time.perf_counter() - started) * 1000)
                await self._mark_success()
            except Exception as exc:  # pragma: no cover
                self._logger.warning("heartbeat failed", extra={"extra_data": {"error": str(exc)}})
                await self._mark_failure()
            await asyncio.sleep(self._config.heartbeat_interval_seconds)

    async def _connect(self) -> None:
        async with self._channel_lock:
            if self._channel is not None:
                await self._channel.close()
            self._channel = grpc.aio.insecure_channel(
                self._config.coordinator_target,
                options=[
                    ("grpc.keepalive_time_ms", 10_000),
                    ("grpc.keepalive_timeout_ms", 5_000),
                    ("grpc.keepalive_permit_without_calls", 1),
                    ("grpc.http2.max_pings_without_data", 0),
                ],
            )
            self._stub = worker_node_pb2_grpc.CoordinatorCallbackServiceStub(self._channel)

    async def _ensure_stub(self) -> worker_node_pb2_grpc.CoordinatorCallbackServiceStub:
        if self._stub is None:
            await self._connect()
        return self._stub  # type: ignore[return-value]

    async def _mark_success(self) -> None:
        self._failure_count = 0
        self._connected = True
        self._metrics.set_coordinator_connected(True)

    async def _mark_failure(self) -> None:
        self._failure_count += 1
        self._connected = False
        self._metrics.set_coordinator_connected(False)
        if self._failure_count >= self._config.coordinator_failure_threshold:
            await self._connect()

    def _retry_delay(self) -> float:
        delay = self._config.coordinator_reconnect_base_seconds * (2 ** max(0, self._failure_count - 1))
        return min(delay, self._config.coordinator_reconnect_max_seconds)
