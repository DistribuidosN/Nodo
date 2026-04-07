# Worker Architecture

Ruta recomendada para leer el proyecto:

1. `worker/server.py`
   Punto de entrada real del worker. Levanta gRPC, health y metricas.

2. `worker/core/worker_runtime.py`
   Runtime principal del nodo. Administra cola, estado, retries y shutdown.

3. `worker/grpc/image_node_service.py`
   Servicio gRPC de negocio (`ImageNodeService`).

4. `worker/grpc/worker_control_service.py`
   Servicio gRPC de control (`WorkerControlService`).

5. `worker/execution/`
   Ejecucion real del procesamiento:
   - `execution_manager.py`: orquesta tareas en ejecucion
   - `image_processor.py`: aplica filtros y transformaciones
   - `resource_manager.py`: decide si hay recursos para ejecutar

6. `worker/core/image_job_service.py`
   Traduce solicitudes de negocio a tareas internas del worker.

7. `worker/scheduler/`
   Cola local y priorizacion.

8. `worker/telemetry/`
   Logging, metricas, health y tracing.

## Carpetas

- `worker/core/`
  Logica central del nodo.

- `worker/execution/`
  Procesamiento real de tareas e imagenes.

- `worker/grpc/`
  Interfaces gRPC y adaptadores.

- `worker/models/`
  Tipos, estados y dataclasses.

- `worker/scheduler/`
  Cola de prioridad y estimacion de costo.

- `worker/telemetry/`
  Observabilidad.

## Nota sobre archivos viejos

Algunos nombres anteriores se mantienen por compatibilidad:

- `worker/main.py`
- `worker/core/node.py`
- `worker/core/business_api.py`
- `worker/grpc/business_servicer.py`
- `worker/grpc/servicer.py`
- `worker/grpc/reporter.py`

Siguen funcionando, pero para leer el proyecto es mejor empezar por los
modulos nuevos con nombres mas claros.
