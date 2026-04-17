from __future__ import annotations

import asyncio
import contextlib
import math
import signal

import grpc

from proto import orchestrator_pb2_grpc
from worker.config import WorkerConfig
from worker.grpc.orchestrator_worker_node_service import OrchestratorWorkerNodeServicer
from worker.telemetry.logging import configure_logging
from worker.telemetry.metrics import WorkerMetrics

# Nuevas importaciones del hexágono
from worker.infrastructure.adapters.storage.local_storage_adapter import LocalStorageAdapter
from worker.infrastructure.adapters.image.pillow_adapter import PillowAdapter
from worker.infrastructure.adapters.grpc.grpc_coordinator_adapter import GrpcCoordinatorAdapter
from worker.application.services.communication_service import CommunicationService
from worker.core.node import WorkerNode

# Este es el punto de entrada principal del Worker de Python.
# Se encarga de instanciar el nodo, configurar el servidor gRPC y 
# registrar los servicios necesarios para la comunicación con el Orquestador Java.

async def run_worker_server() -> None:
    """Configura e inicia el servidor gRPC del Worker."""
    
    # Cargamos la configuración desde variables de entorno
    config = WorkerConfig.from_env()
    
    # Configuración básica de logs
    configure_logging(config.log_level)
    
    # Inicializamos las métricas internas (sin servidor HTTP externo)
    metrics = WorkerMetrics()
    
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
    
    await node.start()

    # Creamos el servidor gRPC asíncrono
    server = grpc.aio.server()
    
    # REGISTRO DE SERVICIOS gRPC
    # Solo registramos el servicio definido en orchestrator.proto (WorkerNode)
    # que es el que el Orquestador Java llamará para pedir métricas o yield de tareas.
    orchestrator_pb2_grpc.add_WorkerNodeServicer_to_server(
        OrchestratorWorkerNodeServicer(node=node, metrics=metrics),
        server,
    )
    
    # El servidor siempre escucha en modo inseguro por defecto en este puerto
    # (Se puede extender para usar TLS si se configuran los certificados)
    server.add_insecure_port(config.bind_target)
    
    await server.start()
    print(f"[*] Worker gRPC server started on {config.bind_target}")

    loop = asyncio.get_running_loop()

    def _request_shutdown() -> None:
        """Maneja la petición de apagado controlado."""
        node.shutdown(max(1, math.ceil(config.graceful_shutdown_timeout_seconds)))

    # Capturamos señales de interrupción (Ctrl+C) o terminación de Docker
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_shutdown)

    try:
        # Esperamos a que el nodo termine su ciclo de vida (vía shutdown o error)
        await node.wait_for_stop()
    finally:
        # Limpieza final de recursos
        with contextlib.suppress(Exception):
            await server.stop(grace=5)
        await node.close()


def main() -> None:
    """Entry point para ejecutar el worker con python -m worker.server"""
    asyncio.run(run_worker_server())


if __name__ == "__main__":
    main()
