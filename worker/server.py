from __future__ import annotations

import asyncio
import contextlib
import math
import signal

import grpc

from proto import orchestrator_pb2_grpc
from worker.config import WorkerConfig
from worker.core.worker_runtime import WorkerNode
from worker.grpc.orchestrator_worker_node_service import OrchestratorWorkerNodeServicer
from worker.telemetry.logging import configure_logging
from worker.telemetry.metrics import WorkerMetrics

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
    
    # Instanciamos el nodo de trabajo principal
    node = WorkerNode(config=config, metrics=metrics)
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
