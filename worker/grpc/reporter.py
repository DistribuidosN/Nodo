from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any, TypedDict, cast

import grpc
import psutil
from PIL import Image

from proto import orchestrator_pb2, orchestrator_pb2_grpc
from worker.config import WorkerConfig
from worker.core.filter_parser import infer_output_format, parse_filters
from worker.core.state_store import EventSpoolStore
from worker.core.storage import StorageClient
from worker.grpc.security import build_channel_credentials
from worker.models.types import ExecutionResultRecord, InputImageRef, NodeState, ProgressEventRecord, Task
from worker.telemetry.metrics import WorkerMetrics
from worker.telemetry import tracing as _tracing_helpers
from worker.telemetry.tracing import grpc_metadata_from_internal, inject_current_context


StatusProvider = Callable[[], Awaitable[NodeState]]
SubmitTaskProvider = Callable[[Task], Awaitable[dict[str, object]]]
StatsProvider = Callable[[], dict[str, int | float]]
BackgroundTask = asyncio.Task[None]
PROTO = cast(Any, orchestrator_pb2)
PROTO_GRPC = cast(Any, orchestrator_pb2_grpc)
TRACING_HELPERS = cast(Any, _tracing_helpers)
MAYBE_SPAN = TRACING_HELPERS.maybe_span


class _ImageTaskPayload(TypedDict):
    task_id: str
    filename: str
    filter_type: str
    target_width: int
    target_height: int
    enqueue_ts: int
    priority: int


class CoordinatorReporter:
    def __init__(
        self,
        config: WorkerConfig,
        metrics: WorkerMetrics,
        status_provider: StatusProvider,
        storage: StorageClient,
        state_root_uri: str,
        submit_task: SubmitTaskProvider,
        stats_provider: StatsProvider,
    ) -> None:
        self._config = config
        self._metrics = metrics
        self._status_provider = status_provider
        self._submit_task = submit_task
        self._stats_provider = stats_provider
        self._logger = logging.getLogger("worker.reporter")
        self._queue: asyncio.Queue[tuple[str, str, ProgressEventRecord | ExecutionResultRecord]] = asyncio.Queue(
            maxsize=config.report_queue_size
        )
        self._stop_event = asyncio.Event()
        self._channel: grpc.aio.Channel | None = None
        self._stub: Any | None = None
        self._tasks: list[BackgroundTask] = []
        self._failure_count = 0
        self._connected = False
        self._channel_lock = asyncio.Lock()
        self._spool = EventSpoolStore(storage, state_root_uri)
        self._storage = storage
        self._enabled = bool(config.coordinator_target)
        self._ip_address = self._resolve_ip_address()

    async def start(self) -> None:
        if not self._enabled:
            self._logger.info("worker reporter disabled; no coordinator target configured")
            self._metrics.set_coordinator_connected(False)
            self._metrics.set_report_queue_length(0)
            return
        await self._connect()
        for item in self._spool.load_pending():
            await self._queue.put((item.spool_id, item.kind, item.payload))
        self._metrics.set_report_queue_length(self._queue.qsize())
        self._tasks = [
            asyncio.create_task(self._report_loop(), name="coordinator-report-loop"),
            asyncio.create_task(self._heartbeat_loop(), name="coordinator-heartbeat-loop"),
            asyncio.create_task(self._pull_loop(), name="coordinator-pull-loop"),
        ]

    async def stop(self) -> None:
        if not self._enabled:
            return
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
        if not self._enabled:
            return
        inject_current_context(event.metadata)
        event.metadata["spool_kind"] = "progress"
        spool_id = self._spool.append("progress", event)
        event.metadata["spool_id"] = spool_id
        await self._queue.put((spool_id, "progress", event))
        self._metrics.set_report_queue_length(self._queue.qsize())

    async def report_result(self, result: ExecutionResultRecord) -> None:
        if not self._enabled:
            return
        inject_current_context(result.metadata)
        result.metadata["spool_kind"] = "result"
        spool_id = self._spool.append("result", result)
        result.metadata["spool_id"] = spool_id
        await self._queue.put((spool_id, "result", result))
        self._metrics.set_report_queue_length(self._queue.qsize())

    @property
    def connected(self) -> bool:
        return self._connected

    @property
    def enabled(self) -> bool:
        return self._enabled

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
        metadata = grpc_metadata_from_internal(payload.metadata)
        with MAYBE_SPAN(f"worker.callback.{kind}", attributes={"worker.callback.kind": kind}):
            if kind == "progress":
                progress_payload = cast(ProgressEventRecord, payload)
                await stub.UpdateTaskProgress(
                    self._progress_to_proto(progress_payload),
                    wait_for_ready=True,
                    metadata=metadata,
                )
            else:
                result_payload = cast(ExecutionResultRecord, payload)
                await stub.SubmitResult(
                    await self._result_to_proto(result_payload),
                    wait_for_ready=True,
                    metadata=metadata,
                )
        self._metrics.grpc_rtt_ms.observe((time.perf_counter() - started) * 1000)
        self._mark_success()

    async def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                status = await self._status_provider()
                stub = await self._ensure_stub()
                started = time.perf_counter()
                with MAYBE_SPAN("worker.callback.heartbeat", attributes={"worker.callback.kind": "heartbeat"}):
                    await stub.SendHeartbeat(
                        PROTO.OrchestratorHeartbeatRequest(
                            node_id=status.node_id,
                            ip_address=self._ip_address,
                            metrics=await self._build_node_metrics(status),
                        ),
                        wait_for_ready=True,
                    )
                self._metrics.grpc_rtt_ms.observe((time.perf_counter() - started) * 1000)
                self._mark_success()
            except Exception as exc:  # pragma: no cover
                self._logger.warning("heartbeat failed", extra={"extra_data": {"error": str(exc)}})
                await self._mark_failure()
            await asyncio.sleep(self._config.heartbeat_interval_seconds)

    async def _pull_loop(self) -> None:
        while not self._stop_event.is_set():
            delay = 1.0
            try:
                status = await self._status_provider()
                slots_free = max(self._config.max_active_tasks - status.active_tasks, 0)
                if slots_free > 0:
                    stub = await self._ensure_stub()
                    response = await stub.PullTasks(
                        PROTO.PullRequest(
                            node_id=status.node_id,
                            slots_free=slots_free,
                            metrics=await self._build_node_metrics(status),
                        ),
                        wait_for_ready=True,
                    )
                    self._mark_success()
                    for image_task in response.tasks:
                        await self._accept_pulled_task(image_task)
                    delay = 0.25 if response.tasks else 1.0
                else:
                    delay = 0.5
            except Exception as exc:  # pragma: no cover
                self._logger.warning("pull failed", extra={"extra_data": {"error": str(exc)}})
                await self._mark_failure()
                delay = self._retry_delay()
            await asyncio.sleep(delay)

    async def _accept_pulled_task(self, image_task: Any) -> None:
        try:
            task = self._task_from_image_task(image_task)
        except Exception as exc:
            await self._submit_mapping_failure(image_task, str(exc))
            return

        reply = await self._submit_task(task)
        if bool(reply.get("accepted")):
            return

        await self._submit_mapping_failure(
            image_task,
            str(reply.get("reason") or "worker rejected pulled task"),
        )

    async def _submit_mapping_failure(self, image_task: Any, message: str) -> None:
        stub = await self._ensure_stub()
        metrics = await self._build_node_metrics()
        await stub.SubmitResult(
            PROTO.TaskResult(
                task_id=str(image_task.task_id),
                node_id=self._config.node_id,
                worker_id=f"{self._config.node_id}-rejected",
                success=False,
                result_data=b"",
                error_msg=message,
                start_ts=0,
                finish_ts=int(datetime.now(tz=UTC).timestamp() * 1000),
                processing_ms=0,
                metrics=metrics,
            ),
            wait_for_ready=True,
        )
        self._mark_success()

    async def _build_node_metrics(self, status: NodeState | None = None) -> Any:
        state = status if status is not None else await self._status_provider()
        memory = psutil.virtual_memory()
        snapshot = self._metrics.snapshot()
        orchestrator_stats = self._stats_provider()
        status_value = self._classify_status(state, orchestrator_stats)
        return PROTO.NodeMetrics(
            node_id=state.node_id,
            ip_address=self._ip_address,
            ram_used_mb=float((memory.total - memory.available) / (1024 * 1024)),
            ram_total_mb=float(memory.total / (1024 * 1024)),
            cpu_percent=float(state.cpu_utilization * 100.0),
            workers_busy=int(state.active_tasks),
            workers_total=int(self._config.max_active_tasks),
            queue_size=int(state.queue_length),
            queue_capacity=int(min(self._config.queue_high_watermark, self._config.max_queue_size)),
            tasks_done=int(orchestrator_stats["tasks_done"]),
            steals_performed=int(orchestrator_stats["steals_performed"]),
            avg_latency_ms=float(snapshot.get("avg_latency_ms", 0.0)),
            p95_latency_ms=float(snapshot.get("p95_latency_ms", 0.0)),
            uptime_seconds=int(orchestrator_stats["uptime_seconds"]),
            status=status_value,
        )

    def _progress_to_proto(self, event: ProgressEventRecord) -> Any:
        worker_id = event.metadata.get("worker_id") or f"{event.node_id}-worker"
        return PROTO.TaskProgress(
            task_id=event.task_id,
            worker_id=worker_id,
            progress_percentage=event.progress_pct,
            status_message=event.message,
        )

    async def _result_to_proto(self, result: ExecutionResultRecord) -> Any:
        payload = b""
        if result.state.value == "succeeded" and result.output_path:
            payload = self._storage.read_bytes(result.output_path)
        started_ms = int(result.started_at.timestamp() * 1000) if result.started_at is not None else 0
        finished_ms = int(result.finished_at.timestamp() * 1000) if result.finished_at is not None else 0
        processing_ms = int(result.metadata.get("processing_ms", "0"))
        metrics = await self._build_node_metrics()
        worker_id = result.metadata.get("worker_id")
        if not worker_id:
            child_pid = result.metadata.get("child_pid")
            worker_id = f"{result.node_id}-pid-{child_pid}" if child_pid else f"{result.node_id}-worker"
        return PROTO.TaskResult(
            task_id=result.task_id,
            node_id=result.node_id,
            worker_id=worker_id,
            success=result.state.value == "succeeded",
            result_data=payload,
            error_msg=result.error_message or "",
            start_ts=started_ms,
            finish_ts=finished_ms,
            processing_ms=processing_ms,
            metrics=metrics,
        )

    def _task_from_image_task(self, image_task: Any) -> Task:
        payload = bytes(image_task.image_data)
        if not payload:
            raise ValueError("pulled task does not contain image bytes")
        width, height, detected_format = self._inspect_image(payload)
        filters = self._filters_from_image_task(image_task)
        transforms, explicit_output_format = parse_filters(filters)
        output_format = explicit_output_format or detected_format or infer_output_format(str(image_task.filename), "png")
        created_at = datetime.fromtimestamp(max(int(image_task.enqueue_ts), 0) / 1000, tz=UTC) if int(image_task.enqueue_ts) > 0 else datetime.now(tz=UTC)
        priority = 9 if int(image_task.priority) > 0 else 5
        filter_type = str(image_task.filter_type or "").strip().lower()
        return Task(
            task_id=str(image_task.task_id),
            idempotency_key=f"orchestrator:{image_task.task_id}",
            priority=priority,
            created_at=created_at,
            deadline=None,
            max_retries=1,
            transforms=transforms,
            input_image=InputImageRef(
                image_id=f"image-{image_task.task_id}",
                payload=payload,
                image_format=detected_format,
                size_bytes=len(payload),
                width=width,
                height=height,
            ),
            output_format=output_format,
            metadata={
                "source_file_name": str(image_task.filename),
                "orchestrator_filter_type": filter_type,
                "orchestrator_enqueue_ts": str(int(image_task.enqueue_ts)),
                "worker_id": f"{self._config.node_id}-worker",
            },
        )

    def _filters_from_image_task(self, image_task: Any) -> list[str]:
        filter_type = str(image_task.filter_type or "").strip().lower()
        filters: list[str] = []
        if filter_type in {"", "none"}:
            pass
        elif filter_type == "thumbnail":
            if int(image_task.target_width) <= 0 or int(image_task.target_height) <= 0:
                raise ValueError("thumbnail task requires target_width and target_height")
        elif filter_type == "grayscale":
            filters.append("grayscale")
        elif filter_type == "ocr":
            filters.append("ocr")
        elif filter_type == "inference":
            filters.append("inference")
        else:
            raise ValueError(f"unsupported orchestrator filter_type '{filter_type}'")

        if int(image_task.target_width) > 0 and int(image_task.target_height) > 0:
            filters.append(f"resize:{int(image_task.target_width)}x{int(image_task.target_height)}")
        return filters

    def _inspect_image(self, payload: bytes) -> tuple[int, int, str]:
        with Image.open(BytesIO(payload)) as image:
            width, height = image.size
            detected_format = (image.format or "png").lower()
        return width, height, detected_format

    async def _connect(self) -> None:
        async with self._channel_lock:
            target = self._config.coordinator_target
            if target is None:
                raise RuntimeError("coordinator target is required when reporter is enabled")
            if self._channel is not None:
                await self._channel.close()
            options: list[tuple[str, int | str]] = [
                ("grpc.keepalive_time_ms", 10_000),
                ("grpc.keepalive_timeout_ms", 5_000),
                ("grpc.keepalive_permit_without_calls", 1),
                ("grpc.http2.max_pings_without_data", 0),
            ]
            if self._config.coordinator_server_name_override:
                options.append(("grpc.ssl_target_name_override", self._config.coordinator_server_name_override))
            channel_credentials = build_channel_credentials(
                root_ca_file=self._config.coordinator_ca_file,
                cert_chain_file=self._config.coordinator_client_cert_file,
                private_key_file=self._config.coordinator_client_key_file,
            )
            if channel_credentials is None:
                self._channel = grpc.aio.insecure_channel(target, options=options)
            else:
                self._channel = grpc.aio.secure_channel(target, channel_credentials, options=options)
            self._stub = PROTO_GRPC.OrchestratorStub(self._channel)

    async def _ensure_stub(self) -> Any:
        if self._stub is None:
            await self._connect()
        return self._stub

    def _mark_success(self) -> None:
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

    def _resolve_ip_address(self) -> str:
        host = self._config.bind_host.strip()
        if host and host not in {"0.0.0.0", "::", "127.0.0.1", "localhost"}:
            return host
        with contextlib.suppress(OSError):
            return socket.gethostbyname(socket.gethostname())
        return "127.0.0.1"

    def _classify_status(self, state: NodeState, orchestrator_stats: dict[str, int | float]) -> str:
        if state.mode.value != "active" or not state.accepting_tasks:
            return "ERROR"
        if float(orchestrator_stats["recent_steal_age_seconds"]) <= 5.0:
            return "STEALING"
        if state.active_tasks > 0 or state.queue_length > 0:
            return "BUSY"
        return "IDLE"
