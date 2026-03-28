from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import grpc

from coordinator.config import CoordinatorConfig
from proto import imagenode_pb2, imagenode_pb2_grpc, worker_node_pb2, worker_node_pb2_grpc
from worker.grpc.security import build_channel_credentials


@dataclass(slots=True)
class ProcessedRecord:
    file_name: str
    result_path: str = ""
    image_data: bytes | None = None
    node_id: str = ""
    message: str = ""
    updated_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))


@dataclass(slots=True)
class WorkerEndpointState:
    node_id: str
    target: str
    control_channel: grpc.aio.Channel
    business_channel: grpc.aio.Channel
    control_stub: worker_node_pb2_grpc.WorkerControlServiceStub
    business_stub: imagenode_pb2_grpc.ImageNodeServiceStub
    status: worker_node_pb2.NodeStatus | None = None
    last_seen_monotonic: float = 0.0
    dispatches: int = 0
    failures: int = 0
    connected: bool = False


@dataclass(slots=True)
class QueuedJob:
    sequence: int
    method: str
    request: Any
    future: asyncio.Future
    file_name: str
    submitted_at_monotonic: float = field(default_factory=time.monotonic)


class CoordinatorRuntime:
    def __init__(self, config: CoordinatorConfig) -> None:
        self._config = config
        self._logger = logging.getLogger("coordinator.runtime")
        self._stop_event = asyncio.Event()
        self._queue: asyncio.Queue[QueuedJob] = asyncio.Queue()
        self._processed: dict[str, ProcessedRecord] = {}
        self._progress_events: list[worker_node_pb2.ProgressEvent] = []
        self._result_events: list[worker_node_pb2.ExecutionResult] = []
        self._heartbeats: dict[str, worker_node_pb2.NodeStatus] = {}
        self._metrics = {
            "submitted_total": 0.0,
            "dispatched_total": 0.0,
            "dispatch_failures_total": 0.0,
            "callback_progress_total": 0.0,
            "callback_result_total": 0.0,
            "heartbeat_total": 0.0,
        }
        self._sequence = 0
        self._dispatch_tasks: list[asyncio.Task] = []
        self._poller_task: asyncio.Task | None = None
        self._workers = self._build_workers()

    async def start(self) -> None:
        self._poller_task = asyncio.create_task(self._poll_status_loop(), name="coordinator-status-poller")
        for index in range(self._config.dispatch_concurrency):
            self._dispatch_tasks.append(asyncio.create_task(self._dispatch_loop(index), name=f"coordinator-dispatch-{index}"))

    async def close(self) -> None:
        self._stop_event.set()
        if self._poller_task is not None:
            self._poller_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poller_task
        for task in self._dispatch_tasks:
            task.cancel()
        if self._dispatch_tasks:
            await asyncio.gather(*self._dispatch_tasks, return_exceptions=True)
        for worker in self._workers.values():
            await worker.control_channel.close()
            await worker.business_channel.close()

    async def process_to_data(self, request: imagenode_pb2.ProcessRequest) -> imagenode_pb2.DataResponse:
        return await self._submit_job("data", request)

    async def process_to_path(self, request: imagenode_pb2.ProcessRequest) -> imagenode_pb2.PathResponse:
        return await self._submit_job("path", request)

    async def upload_large_image(self, request_iterator) -> imagenode_pb2.DataResponse:
        payload = bytearray()
        file_name = ""
        filters: list[str] = []
        async for chunk in request_iterator:
            payload.extend(chunk.chunk_data)
            if chunk.file_name:
                file_name = chunk.file_name
            if chunk.filters:
                filters = list(chunk.filters)
        request = imagenode_pb2.ProcessRequest(
            image_data=bytes(payload),
            file_name=file_name,
            filters=filters,
        )
        return await self.process_to_data(request)

    async def process_batch(self, requests) -> imagenode_pb2.BatchDataResponse:
        responses = await asyncio.gather(*(self.process_to_data(item) for item in requests))
        return imagenode_pb2.BatchDataResponse(
            responses=responses,
            all_success=all(item.success for item in responses),
        )

    async def stream_batch(self, request_iterator):
        pending: set[asyncio.Task] = set()
        queue: asyncio.Queue[object] = asyncio.Queue()
        stop_marker = object()

        async def handle(request: imagenode_pb2.ProcessRequest) -> None:
            try:
                response = await self.process_to_data(request)
            except Exception as exc:
                response = imagenode_pb2.DataResponse(success=False, file_name=request.file_name, message=str(exc))
            await queue.put(response)

        async def producer() -> None:
            try:
                async for request in request_iterator:
                    task = asyncio.create_task(handle(request))
                    pending.add(task)
                    task.add_done_callback(pending.discard)
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
            finally:
                await queue.put(stop_marker)

        producer_task = asyncio.create_task(producer())
        try:
            while True:
                item = await queue.get()
                if item is stop_marker:
                    break
                yield item
        finally:
            producer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await producer_task

    async def health_check(self) -> imagenode_pb2.HealthCheckResponse:
        ready_workers = self.ready_workers
        return imagenode_pb2.HealthCheckResponse(
            is_alive=True,
            is_ready=ready_workers > 0,
            node_id=self._config.node_id,
            message=f"known_workers={len(self._workers)} ready_workers={ready_workers} queue={self._queue.qsize()}",
        )

    async def get_metrics(self) -> imagenode_pb2.MetricsResponse:
        statistics = dict(self._metrics)
        statistics.update(
            {
                "queue_length": float(self._queue.qsize()),
                "known_workers": float(len(self._workers)),
                "ready_workers": float(self.ready_workers),
                "processed_records": float(len(self._processed)),
            }
        )
        for worker in self._workers.values():
            suffix = worker.node_id.replace("-", "_")
            statistics[f"worker_{suffix}_dispatches"] = float(worker.dispatches)
            statistics[f"worker_{suffix}_failures"] = float(worker.failures)
            if worker.status is not None:
                statistics[f"worker_{suffix}_queue_length"] = float(worker.status.queue_length)
                statistics[f"worker_{suffix}_active_tasks"] = float(worker.status.active_tasks)
                statistics[f"worker_{suffix}_capacity"] = float(worker.status.capacity_effective)
        return imagenode_pb2.MetricsResponse(statistics=statistics)

    def get_processed_file_paths(self) -> list[str]:
        return sorted({record.result_path for record in self._processed.values() if record.result_path})

    async def find_path_by_name(self, file_name: str) -> str | None:
        if record := self._processed.get(file_name):
            return record.result_path or None
        for worker in self._workers.values():
            try:
                response = await worker.business_stub.FindPathByName(
                    imagenode_pb2.FileNameRequest(file_name=file_name),
                    timeout=self._config.rpc_timeout_seconds,
                )
            except grpc.RpcError:
                continue
            if response.success and response.result_path:
                self._processed[file_name] = ProcessedRecord(file_name=file_name, result_path=response.result_path, node_id=worker.node_id)
                return response.result_path
        return None

    async def get_processed_images(self) -> list[bytes]:
        images: list[bytes] = []
        for file_name in list(self._processed):
            payload = await self.find_image_by_name(file_name)
            if payload:
                images.append(payload)
        return images

    async def find_image_by_name(self, file_name: str) -> bytes | None:
        record = self._processed.get(file_name)
        if record is not None and record.image_data is not None:
            return record.image_data
        preferred_workers: list[WorkerEndpointState] = []
        if record is not None and record.node_id and record.node_id in self._workers:
            preferred_workers.append(self._workers[record.node_id])
        preferred_workers.extend(worker for worker in self._workers.values() if worker not in preferred_workers)
        for worker in preferred_workers:
            try:
                response = await worker.business_stub.FindImageByName(
                    imagenode_pb2.FileNameRequest(file_name=file_name),
                    timeout=self._config.rpc_timeout_seconds,
                )
            except grpc.RpcError:
                continue
            if response.success and response.image_data:
                self._cache_success(file_name, worker.node_id, response)
                return response.image_data
        return None

    async def record_progress(self, request: worker_node_pb2.ProgressEvent) -> worker_node_pb2.ReportAck:
        self._progress_events.append(request)
        self._metrics["callback_progress_total"] += 1.0
        return worker_node_pb2.ReportAck(accepted=True, message="progress recorded")

    async def record_result(self, request: worker_node_pb2.ExecutionResult) -> worker_node_pb2.ReportAck:
        self._result_events.append(request)
        self._metrics["callback_result_total"] += 1.0
        file_name = request.metadata.get("source_file_name")
        if file_name:
            record = self._processed.get(file_name) or ProcessedRecord(file_name=file_name)
            record.node_id = request.node_id
            record.result_path = request.output_path or record.result_path
            record.message = request.error_message or record.message
            record.updated_at = datetime.now(tz=UTC)
            self._processed[file_name] = record
        return worker_node_pb2.ReportAck(accepted=True, message="result recorded")

    async def record_heartbeat(self, request: worker_node_pb2.HeartbeatRequest) -> worker_node_pb2.HeartbeatReply:
        self._metrics["heartbeat_total"] += 1.0
        status = request.status
        self._heartbeats[status.node_id] = status
        if worker := self._workers.get(status.node_id):
            worker.status = status
            worker.last_seen_monotonic = time.monotonic()
            worker.connected = True
        return worker_node_pb2.HeartbeatReply(accepted=True, message="heartbeat recorded")

    @property
    def ready_workers(self) -> int:
        return sum(1 for worker in self._workers.values() if self._is_ready(worker))

    def get_record(self, file_name: str) -> ProcessedRecord | None:
        return self._processed.get(file_name)

    def _build_workers(self) -> dict[str, WorkerEndpointState]:
        workers: dict[str, WorkerEndpointState] = {}
        credentials = build_channel_credentials(
            root_ca_file=self._config.worker_ca_file,
            cert_chain_file=self._config.worker_client_cert_file,
            private_key_file=self._config.worker_client_key_file,
        )
        options = []
        if self._config.worker_server_name_override:
            options.append(("grpc.ssl_target_name_override", self._config.worker_server_name_override))
        for node_id, target in self._config.workers.items():
            if credentials is None:
                control_channel = grpc.aio.insecure_channel(target)
                business_channel = grpc.aio.insecure_channel(target)
            else:
                control_channel = grpc.aio.secure_channel(target, credentials, options=options)
                business_channel = grpc.aio.secure_channel(target, credentials, options=options)
            workers[node_id] = WorkerEndpointState(
                node_id=node_id,
                target=target,
                control_channel=control_channel,
                business_channel=business_channel,
                control_stub=worker_node_pb2_grpc.WorkerControlServiceStub(control_channel),
                business_stub=imagenode_pb2_grpc.ImageNodeServiceStub(business_channel),
            )
        return workers

    async def _submit_job(self, method: str, request):
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        sequence = self._sequence
        self._sequence += 1
        self._metrics["submitted_total"] += 1.0
        await self._queue.put(
            QueuedJob(
                sequence=sequence,
                method=method,
                request=request,
                future=future,
                file_name=request.file_name or f"anonymous-{sequence}",
            )
        )
        return await future

    async def _dispatch_loop(self, _: int) -> None:
        while not self._stop_event.is_set():
            job = await self._queue.get()
            try:
                response = await self._dispatch_job(job)
                if not job.future.done():
                    job.future.set_result(response)
            except Exception as exc:
                if not job.future.done():
                    if job.method == "path":
                        job.future.set_result(imagenode_pb2.PathResponse(success=False, message=str(exc)))
                    else:
                        job.future.set_result(imagenode_pb2.DataResponse(success=False, file_name=job.file_name, message=str(exc)))
            finally:
                self._queue.task_done()

    async def _dispatch_job(self, job: QueuedJob):
        deadline = time.monotonic() + self._config.rpc_timeout_seconds
        tried: set[str] = set()
        while time.monotonic() < deadline:
            worker = self._select_worker(tried)
            if worker is None:
                tried.clear()
                await asyncio.sleep(self._config.dispatch_wait_seconds)
                continue
            tried.add(worker.node_id)
            try:
                if job.method == "path":
                    response = await worker.business_stub.ProcessToPath(job.request, timeout=self._config.rpc_timeout_seconds)
                    if response.success and response.result_path:
                        self._cache_path(job.file_name, worker.node_id, response)
                    if response.success or not self._is_retryable_failure(response.message):
                        self._metrics["dispatched_total"] += 1.0
                        worker.dispatches += 1
                        return response
                else:
                    response = await worker.business_stub.ProcessToData(job.request, timeout=self._config.rpc_timeout_seconds)
                    if response.success:
                        self._cache_success(job.file_name, worker.node_id, response)
                    if response.success or not self._is_retryable_failure(response.message):
                        self._metrics["dispatched_total"] += 1.0
                        worker.dispatches += 1
                        return response
            except grpc.RpcError as exc:
                worker.failures += 1
                worker.connected = False
                self._metrics["dispatch_failures_total"] += 1.0
                self._logger.warning("worker rpc failed", extra={"extra_data": {"node_id": worker.node_id, "target": worker.target, "error": str(exc)}})
                continue
            worker.failures += 1
            self._metrics["dispatch_failures_total"] += 1.0
            if len(tried) >= len(self._workers):
                tried.clear()
            await asyncio.sleep(self._config.dispatch_wait_seconds)

        if job.method == "path":
            return imagenode_pb2.PathResponse(success=False, message="no available worker before timeout")
        return imagenode_pb2.DataResponse(success=False, file_name=job.file_name, message="no available worker before timeout")

    def _select_worker(self, exclude: set[str]) -> WorkerEndpointState | None:
        candidates = [worker for worker in self._workers.values() if worker.node_id not in exclude]
        ready = [worker for worker in candidates if self._is_ready(worker)]
        pool = ready or candidates
        if not pool:
            return None
        return max(pool, key=self._worker_score)

    def _worker_score(self, worker: WorkerEndpointState) -> tuple[float, float, float, float]:
        if worker.status is None:
            return (-1.0, -1.0, -1.0, -float(worker.dispatches))
        headroom = float(worker.status.capacity_effective) - float(worker.status.active_tasks) - float(worker.status.queue_length)
        utilization_penalty = worker.status.cpu_utilization + worker.status.memory_utilization + worker.status.io_utilization
        freshness = worker.last_seen_monotonic
        return (
            1.0 if self._is_ready(worker) else 0.0,
            headroom,
            -utilization_penalty,
            freshness - float(worker.dispatches),
        )

    def _is_ready(self, worker: WorkerEndpointState) -> bool:
        status = worker.status
        if status is None:
            return False
        if time.monotonic() - worker.last_seen_monotonic > max(self._config.status_poll_seconds * 3.0, 3.0):
            return False
        return bool(status.accepting_tasks and status.mode == worker_node_pb2.NODE_MODE_ACTIVE)

    async def _poll_status_loop(self) -> None:
        while not self._stop_event.is_set():
            for worker in self._workers.values():
                try:
                    status = await worker.control_stub.GetNodeStatus(
                        worker_node_pb2.GetNodeStatusRequest(),
                        timeout=min(self._config.rpc_timeout_seconds, 5.0),
                    )
                except grpc.RpcError:
                    worker.connected = False
                    continue
                worker.status = status
                worker.last_seen_monotonic = time.monotonic()
                worker.connected = True
            await asyncio.sleep(self._config.status_poll_seconds)

    def _cache_success(self, file_name: str, node_id: str, response: imagenode_pb2.DataResponse) -> None:
        self._processed[file_name] = ProcessedRecord(
            file_name=file_name,
            result_path=response.result_path,
            image_data=bytes(response.image_data),
            node_id=node_id,
            message=response.message,
        )

    def _cache_path(self, file_name: str, node_id: str, response: imagenode_pb2.PathResponse) -> None:
        self._processed[file_name] = ProcessedRecord(
            file_name=file_name,
            result_path=response.result_path,
            node_id=node_id,
            message=response.message,
        )

    def _is_retryable_failure(self, message: str) -> bool:
        normalized = message.lower()
        return any(
            token in normalized
            for token in ("rejected", "queue", "watermark", "insufficient memory", "worker rejected")
        )
