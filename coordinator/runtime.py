from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import grpc

from coordinator.config import CoordinatorConfig
from coordinator.state_store import (
    CoordinatorPendingStore,
    CoordinatorProcessedStore,
    PersistedCoordinatorJob,
    PersistedCoordinatorRecord,
)
from proto import imagenode_pb2, imagenode_pb2_grpc, worker_node_pb2, worker_node_pb2_grpc
from worker.core.storage import StorageClient
from worker.grpc.security import build_channel_credentials


@dataclass(slots=True)
class ProcessedRecord:
    request_key: str
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
    dispatch_latency_ewma_ms: float = 0.0


@dataclass(slots=True)
class QueuedJob:
    sequence: int
    request_key: str
    submitted_at_monotonic: float = field(default_factory=time.monotonic)


@dataclass(slots=True)
class RequestWaiter:
    response_kind: str
    future: asyncio.Future


@dataclass(slots=True)
class RequestSlot:
    request_key: str
    request: imagenode_pb2.ProcessRequest
    file_name: str
    preferred_kind: str
    waiters: list[RequestWaiter] = field(default_factory=list)
    owner_node_id: str | None = None
    lease_expires_at: datetime | None = None
    dispatch_attempts: int = 0
    queued: bool = False
    submitted_at: datetime = field(default_factory=lambda: datetime.now(tz=UTC))
    last_error: str = ""


class CoordinatorRuntime:
    def __init__(self, config: CoordinatorConfig) -> None:
        self._config = config
        self._logger = logging.getLogger("coordinator.runtime")
        self._stop_event = asyncio.Event()
        self._queue: asyncio.Queue[QueuedJob] = asyncio.Queue()
        self._processed: dict[str, ProcessedRecord] = {}
        self._processed_by_request_key: dict[str, ProcessedRecord] = {}
        self._request_key_by_file_name: dict[str, str] = {}
        self._slots: dict[str, RequestSlot] = {}
        self._progress_events: list[worker_node_pb2.ProgressEvent] = []
        self._result_events: list[worker_node_pb2.ExecutionResult] = []
        self._heartbeats: dict[str, worker_node_pb2.NodeStatus] = {}
        self._metrics = {
            "submitted_total": 0.0,
            "logical_jobs_total": 0.0,
            "deduplicated_total": 0.0,
            "dispatched_total": 0.0,
            "dispatch_failures_total": 0.0,
            "callback_progress_total": 0.0,
            "callback_result_total": 0.0,
            "heartbeat_total": 0.0,
            "restored_pending_total": 0.0,
            "lease_expired_total": 0.0,
        }
        self._sequence = 0
        self._dispatch_tasks: list[asyncio.Task] = []
        self._poller_task: asyncio.Task | None = None
        self._workers = self._build_workers()
        self._storage = StorageClient(
            endpoint_url=self._config.storage_endpoint_url,
            access_key_id=self._config.storage_access_key_id,
            secret_access_key=self._config.storage_secret_access_key,
            region=self._config.storage_region,
            force_path_style=self._config.storage_force_path_style,
        )
        self._state_root = (
            self._storage.join(self._config.state_uri_prefix, self._config.node_id)
            if self._config.state_uri_prefix
            else str(self._config.state_dir)
        )
        self._pending_store = CoordinatorPendingStore(self._storage, self._state_root)
        self._processed_store = CoordinatorProcessedStore(self._storage, self._state_root)

    async def start(self) -> None:
        self._restore_state()
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
        return await self._submit_request("data", request)

    async def process_to_path(self, request: imagenode_pb2.ProcessRequest) -> imagenode_pb2.PathResponse:
        return await self._submit_request("path", request)

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
            message=f"known_workers={len(self._workers)} ready_workers={ready_workers} queue={self._queue.qsize()} inflight={self.active_ownerships}",
        )

    async def get_metrics(self) -> imagenode_pb2.MetricsResponse:
        statistics = dict(self._metrics)
        statistics.update(
            {
                "queue_length": float(self._queue.qsize()),
                "logical_slots": float(len(self._slots)),
                "known_workers": float(len(self._workers)),
                "ready_workers": float(self.ready_workers),
                "processed_records": float(len(self._processed_by_request_key)),
                "active_ownerships": float(self.active_ownerships),
            }
        )
        for worker in self._workers.values():
            suffix = worker.node_id.replace("-", "_")
            statistics[f"worker_{suffix}_dispatches"] = float(worker.dispatches)
            statistics[f"worker_{suffix}_failures"] = float(worker.failures)
            statistics[f"worker_{suffix}_latency_ewma_ms"] = float(worker.dispatch_latency_ewma_ms)
            statistics[f"worker_{suffix}_ownerships"] = float(self._owned_request_count(worker.node_id))
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
                request_key = self._request_key_by_file_name.get(file_name, f"lookup:{file_name}")
                self._register_processed_record(
                    ProcessedRecord(
                        request_key=request_key,
                        file_name=file_name,
                        result_path=response.result_path,
                        node_id=worker.node_id,
                        message=response.message,
                    ),
                    persist=True,
                )
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
                request_key = self._request_key_by_file_name.get(file_name, f"lookup:{file_name}")
                self._register_processed_record(
                    ProcessedRecord(
                        request_key=request_key,
                        file_name=file_name,
                        result_path=response.result_path,
                        image_data=bytes(response.image_data),
                        node_id=worker.node_id,
                        message=response.message,
                    ),
                    persist=True,
                )
                return response.image_data
        return None

    async def record_progress(self, request: worker_node_pb2.ProgressEvent) -> worker_node_pb2.ReportAck:
        self._progress_events.append(request)
        self._metrics["callback_progress_total"] += 1.0
        return worker_node_pb2.ReportAck(accepted=True, message="progress recorded")

    async def record_result(self, request: worker_node_pb2.ExecutionResult) -> worker_node_pb2.ReportAck:
        self._result_events.append(request)
        self._metrics["callback_result_total"] += 1.0
        file_name = request.metadata.get("source_file_name", "")
        request_key = request.metadata.get("x-coordinator-request-key") or self._request_key_by_file_name.get(file_name)
        if request_key:
            record = self._processed_by_request_key.get(request_key) or ProcessedRecord(
                request_key=request_key,
                file_name=file_name,
            )
            record.node_id = request.node_id
            record.file_name = file_name or record.file_name
            record.result_path = request.output_path or record.result_path
            record.message = request.error_message or record.message
            record.updated_at = datetime.now(tz=UTC)
            self._register_processed_record(record, persist=True)
            if slot := self._slots.get(request_key):
                await self._resolve_slot_from_callback(slot, request)
        elif file_name:
            record = self._processed.get(file_name) or ProcessedRecord(request_key=f"lookup:{file_name}", file_name=file_name)
            record.node_id = request.node_id
            record.result_path = request.output_path or record.result_path
            record.message = request.error_message or record.message
            record.updated_at = datetime.now(tz=UTC)
            self._register_processed_record(record, persist=True)
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

    @property
    def active_ownerships(self) -> int:
        return sum(1 for slot in self._slots.values() if slot.owner_node_id and not self._is_lease_expired(slot))

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

    async def _submit_request(self, response_kind: str, request: imagenode_pb2.ProcessRequest):
        request_key = self._request_key(request)
        self._metrics["submitted_total"] += 1.0
        if record := self._processed_by_request_key.get(request_key):
            return await self._response_from_record(response_kind, record)

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        slot = self._slots.get(request_key)
        if slot is None:
            slot = RequestSlot(
                request_key=request_key,
                request=request,
                file_name=request.file_name or f"anonymous-{self._sequence}",
                preferred_kind=response_kind,
            )
            self._slots[request_key] = slot
            self._metrics["logical_jobs_total"] += 1.0
            self._persist_slot(slot)
            await self._enqueue_slot(slot)
        else:
            self._metrics["deduplicated_total"] += 1.0
            slot.preferred_kind = "data" if response_kind == "data" or slot.preferred_kind == "data" else "path"
            if not slot.file_name:
                slot.file_name = request.file_name
            self._persist_slot(slot)
            if slot.owner_node_id is None and not slot.queued:
                await self._enqueue_slot(slot)
        slot.waiters.append(RequestWaiter(response_kind=response_kind, future=future))
        return await future

    async def _enqueue_slot(self, slot: RequestSlot) -> None:
        if slot.queued:
            return
        sequence = self._sequence
        self._sequence += 1
        slot.queued = True
        self._persist_slot(slot)
        await self._queue.put(QueuedJob(sequence=sequence, request_key=slot.request_key))

    async def _dispatch_loop(self, _: int) -> None:
        while not self._stop_event.is_set():
            job = await self._queue.get()
            try:
                slot = self._slots.get(job.request_key)
                if slot is None:
                    continue
                slot.queued = False
                self._persist_slot(slot)
                await self._dispatch_slot(slot)
            finally:
                self._queue.task_done()

    async def _dispatch_slot(self, slot: RequestSlot) -> None:
        waited_until = time.monotonic() + self._config.rpc_timeout_seconds if slot.waiters else None
        tried: set[str] = set()

        while not self._stop_event.is_set():
            if slot.request_key in self._processed_by_request_key:
                await self._resolve_slot_from_record(slot, self._processed_by_request_key[slot.request_key])
                return

            if slot.owner_node_id and not self._is_lease_expired(slot):
                if waited_until is not None and time.monotonic() >= waited_until:
                    self._fail_current_waiters(slot, "processing still owned by another worker lease")
                    return
                await asyncio.sleep(min(self._config.dispatch_wait_seconds, 0.5))
                continue

            worker = self._select_worker(tried)
            if worker is None:
                if waited_until is not None and time.monotonic() >= waited_until:
                    self._fail_current_waiters(slot, "no available worker before timeout")
                    if not slot.owner_node_id and not slot.queued:
                        await self._enqueue_slot(slot)
                    return
                tried.clear()
                await asyncio.sleep(self._config.dispatch_wait_seconds)
                continue

            tried.add(worker.node_id)
            slot.owner_node_id = worker.node_id
            slot.lease_expires_at = datetime.now(tz=UTC) + timedelta(seconds=self._config.ownership_lease_seconds)
            slot.dispatch_attempts += 1
            self._persist_slot(slot)
            dispatch_kind = "data" if any(waiter.response_kind == "data" for waiter in slot.waiters) else slot.preferred_kind
            metadata = (
                ("x-coordinator-request-key", slot.request_key),
                ("x-coordinator-node-id", self._config.node_id),
            )
            started = time.monotonic()

            try:
                if dispatch_kind == "path":
                    response = await worker.business_stub.ProcessToPath(
                        slot.request,
                        timeout=self._config.rpc_timeout_seconds,
                        metadata=metadata,
                    )
                    latency_ms = (time.monotonic() - started) * 1000.0
                    self._observe_dispatch(worker, latency_ms, response.success)
                    if response.success and response.result_path:
                        self._metrics["dispatched_total"] += 1.0
                        worker.dispatches += 1
                        record = ProcessedRecord(
                            request_key=slot.request_key,
                            file_name=slot.file_name,
                            result_path=response.result_path,
                            node_id=worker.node_id,
                            message=response.message,
                        )
                        self._register_processed_record(record, persist=True)
                        await self._resolve_slot_from_record(slot, record)
                        return
                    if not self._is_retryable_failure(response.message):
                        self._fail_current_waiters(slot, response.message or "worker path request failed")
                        slot.owner_node_id = None
                        slot.lease_expires_at = None
                        self._persist_slot(slot)
                        return
                else:
                    response = await worker.business_stub.ProcessToData(
                        slot.request,
                        timeout=self._config.rpc_timeout_seconds,
                        metadata=metadata,
                    )
                    latency_ms = (time.monotonic() - started) * 1000.0
                    self._observe_dispatch(worker, latency_ms, response.success)
                    if response.success:
                        self._metrics["dispatched_total"] += 1.0
                        worker.dispatches += 1
                        record = ProcessedRecord(
                            request_key=slot.request_key,
                            file_name=slot.file_name,
                            result_path=response.result_path,
                            image_data=bytes(response.image_data),
                            node_id=worker.node_id,
                            message=response.message,
                        )
                        self._register_processed_record(record, persist=True)
                        await self._resolve_slot_from_record(slot, record)
                        return
                    if not self._is_retryable_failure(response.message):
                        self._fail_current_waiters(slot, response.message or "worker data request failed")
                        slot.owner_node_id = None
                        slot.lease_expires_at = None
                        self._persist_slot(slot)
                        return
            except grpc.RpcError as exc:
                worker.failures += 1
                worker.connected = False
                self._metrics["dispatch_failures_total"] += 1.0
                self._observe_dispatch(worker, 0.0, False)
                self._logger.warning(
                    "worker rpc failed",
                    extra={"extra_data": {"node_id": worker.node_id, "target": worker.target, "error": str(exc)}},
                )

            slot.last_error = "retryable worker failure"
            slot.owner_node_id = None
            slot.lease_expires_at = None
            self._persist_slot(slot)
            if len(tried) >= len(self._workers):
                tried.clear()
            if waited_until is not None and time.monotonic() >= waited_until:
                self._fail_current_waiters(slot, "retryable worker failures before timeout")
                if not slot.queued:
                    await self._enqueue_slot(slot)
                return
            await asyncio.sleep(self._config.dispatch_wait_seconds)

    def _select_worker(self, exclude: set[str]) -> WorkerEndpointState | None:
        candidates = [worker for worker in self._workers.values() if worker.node_id not in exclude]
        ready = [worker for worker in candidates if self._is_ready(worker)]
        pool = ready or candidates
        if not pool:
            return None
        return max(pool, key=self._worker_score)

    def _worker_score(self, worker: WorkerEndpointState) -> tuple[float, float, float, float, float, float]:
        if worker.status is None:
            return (-1.0, -1.0, -1.0, -1.0, -1.0, -float(worker.dispatches))
        headroom = float(worker.status.capacity_effective) - float(worker.status.active_tasks) - float(worker.status.queue_length)
        headroom -= float(self._owned_request_count(worker.node_id))
        utilization_penalty = worker.status.cpu_utilization + worker.status.memory_utilization + worker.status.io_utilization
        freshness = worker.last_seen_monotonic
        latency_penalty = worker.dispatch_latency_ewma_ms or 0.0
        return (
            1.0 if self._is_ready(worker) else 0.0,
            headroom,
            -utilization_penalty,
            -latency_penalty,
            -float(worker.failures),
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
            await self._requeue_expired_leases()
            await asyncio.sleep(self._config.status_poll_seconds)

    def _request_key(self, request: imagenode_pb2.ProcessRequest) -> str:
        digest = hashlib.sha256()
        digest.update(request.file_name.encode("utf-8"))
        digest.update(b"\0")
        for item in request.filters:
            digest.update(item.encode("utf-8"))
            digest.update(b"\0")
        digest.update(hashlib.sha256(bytes(request.image_data)).digest())
        return digest.hexdigest()

    def _persist_slot(self, slot: RequestSlot) -> None:
        self._pending_store.upsert(
            PersistedCoordinatorJob(
                request_key=slot.request_key,
                request=slot.request,
                file_name=slot.file_name,
                preferred_kind=slot.preferred_kind,
                owner_node_id=slot.owner_node_id,
                lease_expires_at=slot.lease_expires_at,
                dispatch_attempts=slot.dispatch_attempts,
                queued=slot.queued,
                submitted_at=slot.submitted_at,
                last_error=slot.last_error,
            )
        )

    def _register_processed_record(self, record: ProcessedRecord, *, persist: bool) -> None:
        self._processed_by_request_key[record.request_key] = record
        if record.file_name:
            self._processed[record.file_name] = record
            self._request_key_by_file_name[record.file_name] = record.request_key
        if persist:
            self._processed_store.upsert(
                PersistedCoordinatorRecord(
                    request_key=record.request_key,
                    file_name=record.file_name,
                    result_path=record.result_path,
                    image_data=record.image_data,
                    node_id=record.node_id,
                    message=record.message,
                    updated_at=record.updated_at,
                )
            )

    async def _resolve_slot_from_record(self, slot: RequestSlot, record: ProcessedRecord) -> None:
        waiters = list(slot.waiters)
        slot.waiters.clear()
        for waiter in waiters:
            if waiter.future.done():
                continue
            waiter.future.set_result(await self._response_from_record(waiter.response_kind, record))
        self._pending_store.remove(slot.request_key)
        self._slots.pop(slot.request_key, None)

    async def _resolve_slot_from_callback(self, slot: RequestSlot, request: worker_node_pb2.ExecutionResult) -> None:
        if request.status == worker_node_pb2.TASK_STATUS_SUCCEEDED:
            record = self._processed_by_request_key.get(slot.request_key)
            if record is not None:
                await self._resolve_slot_from_record(slot, record)
                return
        message = request.error_message or "worker finished without success"
        self._fail_current_waiters(slot, message)
        self._pending_store.remove(slot.request_key)
        self._slots.pop(slot.request_key, None)

    def _fail_current_waiters(self, slot: RequestSlot, message: str) -> None:
        waiters = list(slot.waiters)
        slot.waiters.clear()
        for waiter in waiters:
            if waiter.future.done():
                continue
            if waiter.response_kind == "path":
                waiter.future.set_result(imagenode_pb2.PathResponse(success=False, message=message))
            else:
                waiter.future.set_result(imagenode_pb2.DataResponse(success=False, file_name=slot.file_name, message=message))

    async def _response_from_record(self, response_kind: str, record: ProcessedRecord):
        if response_kind == "path":
            return imagenode_pb2.PathResponse(
                result_path=record.result_path,
                success=bool(record.result_path),
                message=record.message or "ok",
            )
        payload = record.image_data
        if payload is None and record.file_name:
            payload = await self.find_image_by_name(record.file_name)
        if payload is None:
            return imagenode_pb2.DataResponse(success=False, file_name=record.file_name, result_path=record.result_path, message="image bytes not available")
        return imagenode_pb2.DataResponse(
            image_data=payload,
            success=True,
            file_name=record.file_name,
            result_path=record.result_path,
            message=record.message or "ok",
        )

    async def _requeue_expired_leases(self) -> None:
        now = datetime.now(tz=UTC)
        for slot in list(self._slots.values()):
            if slot.owner_node_id and slot.lease_expires_at and slot.lease_expires_at <= now:
                slot.owner_node_id = None
                slot.lease_expires_at = None
                self._metrics["lease_expired_total"] += 1.0
                self._persist_slot(slot)
                if not slot.queued:
                    await self._enqueue_slot(slot)

    def _is_lease_expired(self, slot: RequestSlot) -> bool:
        return slot.lease_expires_at is None or slot.lease_expires_at <= datetime.now(tz=UTC)

    def _restore_state(self) -> None:
        for record in self._processed_store.load_all():
            self._register_processed_record(
                ProcessedRecord(
                    request_key=record.request_key,
                    file_name=record.file_name,
                    result_path=record.result_path,
                    image_data=record.image_data,
                    node_id=record.node_id,
                    message=record.message,
                    updated_at=record.updated_at,
                ),
                persist=False,
            )

        now = datetime.now(tz=UTC)
        restored: list[RequestSlot] = []
        for entry in self._pending_store.load_all():
            if entry.request_key in self._processed_by_request_key:
                self._pending_store.remove(entry.request_key)
                continue
            slot = RequestSlot(
                request_key=entry.request_key,
                request=entry.request,
                file_name=entry.file_name,
                preferred_kind=entry.preferred_kind,
                owner_node_id=entry.owner_node_id,
                lease_expires_at=entry.lease_expires_at,
                dispatch_attempts=entry.dispatch_attempts,
                queued=entry.queued,
                submitted_at=entry.submitted_at,
                last_error=entry.last_error,
            )
            if slot.lease_expires_at and slot.lease_expires_at <= now:
                slot.owner_node_id = None
                slot.lease_expires_at = None
            slot.queued = False
            self._slots[slot.request_key] = slot
            restored.append(slot)
        self._metrics["restored_pending_total"] = float(len(restored))
        for slot in restored:
            if slot.owner_node_id is None:
                asyncio.get_running_loop().create_task(self._enqueue_slot(slot))

    def _observe_dispatch(self, worker: WorkerEndpointState, latency_ms: float, success: bool) -> None:
        if not success and latency_ms <= 0:
            return
        alpha = 0.35
        if worker.dispatch_latency_ewma_ms <= 0:
            worker.dispatch_latency_ewma_ms = latency_ms
        else:
            worker.dispatch_latency_ewma_ms = alpha * latency_ms + (1.0 - alpha) * worker.dispatch_latency_ewma_ms

    def _owned_request_count(self, node_id: str) -> int:
        return sum(1 for slot in self._slots.values() if slot.owner_node_id == node_id and not self._is_lease_expired(slot))

    def _is_retryable_failure(self, message: str) -> bool:
        normalized = message.lower()
        return any(
            token in normalized
            for token in ("rejected", "queue", "watermark", "insufficient memory", "worker rejected", "timeout")
        )
