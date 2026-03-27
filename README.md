# Distributed Image Worker

Worker gRPC en Python para procesamiento distribuido de imagenes. Este repositorio implementa solo la parte de nodos worker: recepcion de tareas, cola de prioridad, scheduling local, ejecucion paralela, backpressure, retries, metricas, healthchecks y reporte al coordinador.

## Decisiones principales

- `asyncio + grpc.aio` para control, scheduling y telemetria.
- `Pillow` para la primera version porque cubre `grayscale`, `resize`, `crop`, `rotate`, `flip`, `blur`, `sharpen`, `brightness/contrast`, `watermark/text` y conversion de formato con menor complejidad operativa que OpenCV.
- `ThreadPoolExecutor` para trabajos ligeros y `ProcessPoolExecutor` para transformaciones mas pesadas.
- Cola priorizada con `heapq`, score hibrido por prioridad, deadline, aging, costo estimado y tamano.
- Canal persistente gRPC hacia un coordinador mock para `ReportProgress`, `ReportResult` y `Heartbeat`.
- Persistencia ligera en `data/state/completed_tasks.jsonl` para deduplicacion tras reinicio.
- Persistencia de cola pendiente en `data/state/pending_tasks/` para restaurar tareas en espera o retry al arrancar.
- Spool durable de eventos al coordinador en `data/state/event_spool/`, con replay tras reinicio.
- Healthchecks HTTP en `/livez` y `/readyz` separados del plano gRPC.

## Estructura

```text
proto/
scripts/
worker/
  core/
  execution/
  grpc/
  models/
  scheduler/
  telemetry/
tests/
examples/
```

## Instalacion

```bash
python -m pip install -e .[dev]
```

## Generacion de protos

```bash
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/worker_node.proto
```

## Ejecutar localmente

Terminal 1:

```bash
python examples/mock_coordinator.py
```

Terminal 2:

```bash
python -m worker
```

Terminal 3:

```bash
python examples/submit_task.py
```

Enviar a un worker especifico:

```bash
python examples/submit_task.py --target 127.0.0.1:50061
```

Prueba simple de carga:

```bash
python examples/load_submitter.py --count 100 --concurrency 16
```

## Contenedores

Levantar coordinador mock + 3 workers:

```bash
docker compose up --build
```

Servicios expuestos:

- Coordinator mock: `127.0.0.1:50052`
- Worker 1 gRPC: `127.0.0.1:50051`
- Worker 2 gRPC: `127.0.0.1:50061`
- Worker 3 gRPC: `127.0.0.1:50071`
- Worker 1 health: `http://127.0.0.1:8081/readyz`
- Worker 2 health: `http://127.0.0.1:8082/readyz`
- Worker 3 health: `http://127.0.0.1:8083/readyz`
- Worker 1 metrics: `http://127.0.0.1:9101`
- Worker 2 metrics: `http://127.0.0.1:9102`
- Worker 3 metrics: `http://127.0.0.1:9103`

Cada nodo worker es la misma aplicacion desplegada 3 veces con diferente `WORKER_NODE_ID` y puertos externos distintos.

## Healthchecks y metricas

- `GET /livez`: confirma que el proceso esta vivo.
- `GET /readyz`: confirma que el worker esta en modo `ACTIVE`, con memoria suficiente, por debajo del `HWM` y con conectividad al coordinador.
- Prometheus por defecto en `http://127.0.0.1:9101`, `9102` y `9103`.

Metricas nuevas relevantes:

- `worker_coordinator_connected`
- `worker_readiness`
- `worker_report_queue_length`

## Tests

```bash
python -m pytest
```

## Variables de entorno utiles

- `WORKER_NODE_ID`
- `WORKER_BIND_HOST`
- `WORKER_BIND_PORT`
- `WORKER_COORDINATOR_TARGET`
- `WORKER_OUTPUT_DIR`
- `WORKER_STATE_DIR`
- `WORKER_METRICS_HOST`
- `WORKER_METRICS_PORT`
- `WORKER_HEALTH_HOST`
- `WORKER_HEALTH_PORT`
- `WORKER_MAX_ACTIVE_TASKS`
- `WORKER_QUEUE_HWM`
- `WORKER_QUEUE_LWM`
- `WORKER_MIN_FREE_MEMORY_BYTES`
- `WORKER_COORDINATOR_RECONNECT_BASE_SECONDS`
- `WORKER_COORDINATOR_RECONNECT_MAX_SECONDS`
- `WORKER_COORDINATOR_FAILURE_THRESHOLD`

## Hardening agregado

- Reconexion gRPC con backoff exponencial y `keepalive`.
- Health server listo para Docker y orquestadores.
- Manejo de `SIGINT` y `SIGTERM` para cierre ordenado.
- Persistencia ligera de resultados terminales para deduplicacion tras reinicios.
- Restauracion automatica de cola pendiente desde disco.
- Spool durable de progreso y resultados hasta recibir `ack` del coordinador.
- Cancelacion cooperativa por defecto para las transformaciones integradas del worker. El aislamiento en proceso dedicado queda reservado para extensiones no cooperativas o cuando una tarea lo pida explicitamente con `metadata.execution_isolation=process`.
