from __future__ import annotations

import asyncio
import contextlib
import math
import signal

import grpc

from proto import imagenode_pb2_grpc, worker_node_pb2_grpc
from worker.config import WorkerConfig
<<<<<<< Updated upstream
from worker.core.worker_runtime import WorkerNode
from worker.grpc.image_node_service import ImageNodeBusinessServicer
from worker.grpc.security import build_server_credentials
from worker.grpc.worker_control_service import WorkerControlServicer
=======
from worker.grpc.orchestrator_worker_node_service import OrchestratorWorkerNodeServicer
>>>>>>> Stashed changes
from worker.telemetry.logging import configure_logging
from worker.telemetry.metrics import WorkerMetrics
from worker.telemetry.tracing import configure_tracing, shutdown_tracing

<<<<<<< Updated upstream
=======
# Nuevas importaciones del hexágono
from worker.infrastructure.adapters.storage.local_storage_adapter import LocalStorageAdapter
from worker.infrastructure.adapters.image.pillow_adapter import PillowAdapter
from worker.infrastructure.adapters.grpc.grpc_coordinator_adapter import GrpcCoordinatorAdapter
from worker.application.services.communication_service import CommunicationService
from worker.core.node import WorkerNode

# Este es el punto de entrada principal del Worker de Python.
# Se encarga de instanciar el nodo, configurar el servidor gRPC y 
# registrar los servicios necesarios para la comunicación con el Orquestador Java.
>>>>>>> Stashed changes

async def run_worker_server() -> None:
    config = WorkerConfig.from_env()
    configure_logging(config.log_level)
    configure_tracing(config)
    metrics = WorkerMetrics()
<<<<<<< Updated upstream
    node = WorkerNode(config=config, metrics=metrics)
=======
    
    # 1. Adaptadores de Infraestructura
    storage_adapter = LocalStorageAdapter()
    image_adapter = PillowAdapter(
        storage=storage_adapter,
        ocr_command=config.ocr_command,
        inference_command=config.inference_command
    )
    
    # El adaptador gRPC necesita el objetivo del coordinador de la configuración
    coordinator_adapter = GrpcCoordinatorAdapter(
        target=config.coordinator_target,
        node_id=config.node_id,
        storage=storage_adapter
    )
    
    # 2. Servicios de Aplicación
    # (Nota: El nodo será el status_provider y submit_task_provider del servicio)
    # Por economía de diseño, los vincularemos abajo.
    
    # 3. Instanciamos el nodo de trabajo principal
    node = WorkerNode(
        config=config, 
        metrics=metrics, 
        storage=storage_adapter, 
        processor=image_adapter
    )
    
    comm_service = CommunicationService(
        task_provider=coordinator_adapter,
        result_reporter=coordinator_adapter,
        heartbeat_port=coordinator_adapter,
        status_provider=node.get_node_state,
        submit_task=node.submit_task,
        heartbeat_interval=config.heartbeat_interval_seconds
    )
    
    # Inyectamos el servicio en el nodo
    node._communication = comm_service
    
>>>>>>> Stashed changes
    await node.start()

    server = grpc.aio.server()
    worker_node_pb2_grpc.add_WorkerControlServiceServicer_to_server(WorkerControlServicer(node), server)  # type: ignore[attr-defined]
    imagenode_pb2_grpc.add_ImageNodeServiceServicer_to_server(  # type: ignore[attr-defined]
        ImageNodeBusinessServicer(node=node, metrics=metrics, stream_concurrency=config.max_active_tasks),
        server,
    )
    server_credentials = build_server_credentials(
        cert_chain_file=config.grpc_server_cert_file,
        private_key_file=config.grpc_server_key_file,
        client_ca_file=config.grpc_server_client_ca_file,
        require_client_auth=config.grpc_server_require_client_auth,
    )
    if server_credentials is None:
        server.add_insecure_port(config.bind_target)
    else:
        server.add_secure_port(config.bind_target, server_credentials)
    await server.start()

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        node.shutdown(max(1, math.ceil(config.graceful_shutdown_timeout_seconds)))

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown)

    try:
        await node.wait_for_stop()
    finally:
        with contextlib.suppress(Exception):
            await server.stop(grace=5)
        await node.close()
        shutdown_tracing()


def main() -> None:
    asyncio.run(run_worker_server())


if __name__ == "__main__":
    main()
