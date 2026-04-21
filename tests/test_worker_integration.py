from __future__ import annotations

import asyncio
import socket
from io import BytesIO
from datetime import UTC, datetime

import grpc
import pytest
from PIL import Image

<<<<<<< Updated upstream
from proto import worker_node_pb2, worker_node_pb2_grpc
=======
from proto import orchestrator_pb2, orchestrator_pb2_grpc
>>>>>>> Stashed changes
from worker.config import WorkerConfig
from worker.core.node import WorkerNode
from worker.telemetry.metrics import WorkerMetrics

# Nuevas importaciones hexagonales para el setup del test
from worker.infrastructure.adapters.storage.local_storage_adapter import LocalStorageAdapter
from worker.infrastructure.adapters.grpc.grpc_coordinator_adapter import GrpcCoordinatorAdapter
from worker.application.services.communication_service import CommunicationService


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def build_config(
    tmp_path,
    worker_port: int,
    coordinator_port: int | None,
) -> WorkerConfig:
    return WorkerConfig(
        node_id="integration-node",
        bind_host="127.0.0.1",
        bind_port=worker_port,
        coordinator_target=f"127.0.0.1:{coordinator_port}" if coordinator_port is not None else None,
        input_dir=tmp_path / "input",
        output_dir=tmp_path / "output",
        state_dir=tmp_path / "state",
        max_active_tasks=2,
        process_pool_workers=1,
        cpu_target=0.85,
        max_queue_size=16,
        queue_high_watermark=12,
        queue_low_watermark=8,
        min_free_memory_bytes=64 * 1024 * 1024,
        large_image_threshold_bytes=8 * 1024 * 1024,
        heartbeat_interval_seconds=0.5,
        report_queue_size=64,
        retry_base_ms=100,
        retry_max_ms=1000,
        score_deadline_window_seconds=30.0,
        score_aging_window_seconds=60.0,
        score_cost_window_ms=1500.0,
        scheduler_poll_seconds=0.02,
        dedupe_ttl_seconds=60,
        coordinator_reconnect_base_seconds=0.2,
        coordinator_reconnect_max_seconds=1.0,
        coordinator_failure_threshold=2,
        graceful_shutdown_timeout_seconds=2.0,
        log_level="INFO",
    )


def build_payload() -> bytes:
    image = Image.new("RGB", (160, 120), color=(90, 50, 210))
    buffer = BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


class MockCoordinator(worker_node_pb2_grpc.CoordinatorCallbackServiceServicer):
    def __init__(self) -> None:
        self.progress: list[worker_node_pb2.ProgressEvent] = []
        self.results: list[worker_node_pb2.ExecutionResult] = []
        self.progress_metadata: list[tuple[tuple[str, str], ...]] = []
        self.result_metadata: list[tuple[tuple[str, str], ...]] = []
        self.result_event = asyncio.Event()

    async def ReportProgress(self, request, context):
        self.progress.append(request)
        self.progress_metadata.append(tuple((item.key, item.value) for item in context.invocation_metadata()))
        return worker_node_pb2.ReportAck(accepted=True, message="ok")

    async def ReportResult(self, request, context):
        self.results.append(request)
        self.result_metadata.append(tuple((item.key, item.value) for item in context.invocation_metadata()))
        self.result_event.set()
        return worker_node_pb2.ReportAck(accepted=True, message="ok")

    async def Heartbeat(self, request, context):
        return worker_node_pb2.HeartbeatReply(accepted=True, message="ok")


@pytest.mark.asyncio
<<<<<<< Updated upstream
async def test_worker_processes_and_reports_task(tmp_path):
    worker_port = free_port()
=======
async def test_worker_pulls_task_from_orchestrator_and_submits_result(tmp_path):
    # 1. Setup del puerto y el MockCoordinator
>>>>>>> Stashed changes
    coordinator_port = free_port()
    metrics_port = free_port()
    health_port = free_port()
    coordinator = MockCoordinator()
<<<<<<< Updated upstream

    coordinator_server = grpc.aio.server()
    worker_node_pb2_grpc.add_CoordinatorCallbackServiceServicer_to_server(coordinator, coordinator_server)
    coordinator_server.add_insecure_port(f"127.0.0.1:{coordinator_port}")
    await coordinator_server.start()

    config = build_config(tmp_path, worker_port, coordinator_port, metrics_port, health_port)
    node = WorkerNode(config=config, metrics=WorkerMetrics())
    await node.start()

    worker_server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(node), worker_server)
    worker_server.add_insecure_port(f"127.0.0.1:{worker_port}")
    await worker_server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{worker_port}")
    stub = worker_node_pb2_grpc.WorkerControlServiceStub(channel)

    created_at = Timestamp()
    created_at.FromDatetime(datetime.now(tz=UTC))
    payload = build_payload()
    request = worker_node_pb2.SubmitTaskRequest(
        task=worker_node_pb2.Task(
            task_id="integration-task",
            idempotency_key="integration-task",
            priority=9,
            created_at=created_at,
            max_retries=1,
            output_format=worker_node_pb2.IMAGE_FORMAT_PNG,
            input=worker_node_pb2.InputImage(
                image_id="integration-image",
                content=payload,
                format=worker_node_pb2.IMAGE_FORMAT_PNG,
                size_bytes=len(payload),
                width=160,
                height=120,
            ),
            transforms=[
                worker_node_pb2.Transformation(type=worker_node_pb2.OPERATION_GRAYSCALE),
                worker_node_pb2.Transformation(
                    type=worker_node_pb2.OPERATION_RESIZE,
                    params={"width": "80", "height": "60"},
                ),
            ],
        )
    )

    traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    response = await stub.SubmitTask(request, metadata=(("traceparent", traceparent),))
    assert response.accepted is True

    await asyncio.wait_for(coordinator.result_event.wait(), timeout=10)
    assert coordinator.results
    result = coordinator.results[-1]
    assert result.status == worker_node_pb2.TASK_STATUS_SUCCEEDED
    assert result.width == 80
    assert result.height == 60
    assert len(coordinator.progress) >= 2
    assert any(key == "traceparent" and value == traceparent for key, value in coordinator.result_metadata[-1])

    status = await stub.GetNodeStatus(worker_node_pb2.GetNodeStatusRequest())
    assert status.node_id == "integration-node"

    await channel.close()
    await worker_server.stop(grace=3)
    await node.close()
    await coordinator_server.stop(grace=3)


@pytest.mark.asyncio
async def test_worker_control_supports_webp_output_format_enum(tmp_path):
    worker_port = free_port()
    coordinator_port = free_port()
    metrics_port = free_port()
    health_port = free_port()
    coordinator = MockCoordinator()

    coordinator_server = grpc.aio.server()
    worker_node_pb2_grpc.add_CoordinatorCallbackServiceServicer_to_server(coordinator, coordinator_server)
    coordinator_server.add_insecure_port(f"127.0.0.1:{coordinator_port}")
    await coordinator_server.start()

    config = build_config(tmp_path, worker_port, coordinator_port, metrics_port, health_port)
    node = WorkerNode(config=config, metrics=WorkerMetrics())
    await node.start()

    worker_server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(node), worker_server)
    worker_server.add_insecure_port(f"127.0.0.1:{worker_port}")
    await worker_server.start()

    channel = grpc.aio.insecure_channel(f"127.0.0.1:{worker_port}")
    stub = worker_node_pb2_grpc.WorkerControlServiceStub(channel)

    created_at = Timestamp()
    created_at.FromDatetime(datetime.now(tz=UTC))
    payload = build_payload()
    request = worker_node_pb2.SubmitTaskRequest(
        task=worker_node_pb2.Task(
            task_id="webp-task",
            idempotency_key="webp-task",
            priority=9,
            created_at=created_at,
            max_retries=1,
            output_format=worker_node_pb2.IMAGE_FORMAT_WEBP,
            input=worker_node_pb2.InputImage(
                image_id="webp-image",
                content=payload,
                format=worker_node_pb2.IMAGE_FORMAT_PNG,
                size_bytes=len(payload),
                width=160,
                height=120,
            ),
            transforms=[
                worker_node_pb2.Transformation(
                    type=worker_node_pb2.OPERATION_RESIZE,
                    params={"width": "64", "height": "48"},
                ),
            ],
        )
    )

    response = await stub.SubmitTask(request)
    assert response.accepted is True

    await asyncio.wait_for(coordinator.result_event.wait(), timeout=10)
    result = coordinator.results[-1]
    assert result.status == worker_node_pb2.TASK_STATUS_SUCCEEDED
    assert result.output_format == worker_node_pb2.IMAGE_FORMAT_WEBP
    assert result.output_path.endswith(".webp")
    assert result.width == 64
    assert result.height == 48

    await channel.close()
    await worker_server.stop(grace=3)
    await node.close()
    await coordinator_server.stop(grace=3)


@pytest.mark.asyncio
async def test_worker_can_run_without_embedded_coordinator(tmp_path):
    worker_port = free_port()
    metrics_port = free_port()
    health_port = free_port()

    config = build_config(tmp_path, worker_port, None, metrics_port, health_port)
    node = WorkerNode(config=config, metrics=WorkerMetrics())
    await node.start()

    worker_server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(node), worker_server)
    worker_server.add_insecure_port(f"127.0.0.1:{worker_port}")
    await worker_server.start()

    status = await node.get_node_state()
    health = node.current_health()

    assert status.node_id == "integration-node"
    assert health.live is True
    assert health.ready is True
    assert health.coordinator_connected is False

    await worker_server.stop(grace=3)
    await node.close()
=======
    
    # Preparamos una tarea para ser "comprada" por el worker
    payload = build_payload()
    coordinator.pulled_tasks.append(
        orchestrator_pb2.ImageTask(
            task_id="pulled-task",
            image_data=payload,
            filename="pulled.png",
            filter_type="grayscale",
            target_width=80,
            target_height=60,
            enqueue_ts=int(datetime.now(tz=UTC).timestamp() * 1000),
            priority=1,
        )
    )

    # 2. Iniciamos el servidor Mock del Orquestador
    coordinator_server = grpc.aio.server()
    orchestrator_pb2_grpc.add_OrchestratorServicer_to_server(coordinator, coordinator_server)
    coordinator_server.add_insecure_port(f"127.0.0.1:{coordinator_port}")
    await coordinator_server.start()

    # 3. Configuramos y ensamblamos el Worker de forma Hexagonal
    config = build_config(tmp_path, free_port(), coordinator_port)
    metrics = WorkerMetrics()
    storage = LocalStorageAdapter()
    
    # Adaptador de salida (hacia el orquestador)
    coordinator_adapter = GrpcCoordinatorAdapter(
        target=f"127.0.0.1:{coordinator_port}",
        node_id=config.node_id,
        storage=storage
    )
    
    processor = PillowAdapter(storage)
    node = WorkerNode(config=config, metrics=metrics, storage=storage, processor=processor)
    
    # Servicio de Aplicación que orquesta la comunicación
    comm_service = CommunicationService(
        task_provider=coordinator_adapter,
        result_reporter=coordinator_adapter,
        heartbeat_port=coordinator_adapter,
        status_provider=node.get_node_state,
        submit_task=node.submit_task,
        heartbeat_interval=0.1 # Rápido para el test
    )
    
    # Inyectamos y arrancamos
    node._communication = comm_service
    await node.start()

    # 4. Verificación
    # Esperamos a que el worker haga PULL, procese y haga SUBMIT
    await asyncio.wait_for(coordinator.result_event.wait(), timeout=10)
    
    result = coordinator.results[-1]
    assert result.task_id == "pulled-task"
    assert result.success is True
    assert result.result_data
    assert len(coordinator.pull_requests) > 0
    
    # Verificamos que se haya guardado en el storage local también
    assert (tmp_path / "output" / "pulled-task.png").exists()

    # 5. Cleanup
    await node.close()
    await coordinator_server.stop(grace=1)
>>>>>>> Stashed changes
