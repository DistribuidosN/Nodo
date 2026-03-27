# Distributed Image Worker

Worker gRPC en Python para procesamiento distribuido de imagenes. Este repositorio implementa solo la parte de nodos worker: recepcion de tareas, cola de prioridad, scheduling local, ejecucion paralela, backpressure, retries, metricas, healthchecks y reporte al coordinador.

## Decisiones principales

- `asyncio + grpc.aio` para control, scheduling y telemetria.
- `Pillow` para la primera version porque cubre `grayscale`, `resize`, `crop`, `rotate`, `flip`, `blur`, `sharpen`, `brightness/contrast`, `watermark/text` y conversion de formato con menor complejidad operativa que OpenCV.
- `ThreadPoolExecutor` para el set integrado de transformaciones cooperativas y procesos dedicados solo para aislamiento explicito o extensiones no cooperativas.
- Cola priorizada con `heapq`, score hibrido por prioridad, deadline, aging, costo estimado y tamano.
- Canal persistente gRPC hacia un coordinador mock para `ReportProgress`, `ReportResult` y `Heartbeat`.
- `input_uri` y `output_uri/output_uri_prefix` soportados sobre filesystem local, `file://`, `http(s)` de solo lectura y `s3://`/MinIO.
- Persistencia ligera de resultados, cola pendiente y spool durable sobre filesystem local o un backend compartido configurable por URI.
- OCR e inferencia integrados como adaptadores ejecutables configurables por comando, sin cambiar el contrato gRPC del worker.
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

Ejemplo con `input_uri` y `output_uri_prefix`:

```bash
python examples/submit_task.py --target 127.0.0.1:50051
```

La tarea puede enviar `input.input_uri` en el proto y opcionalmente `metadata["output_uri"]` o `metadata["output_uri_prefix"]`.

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
- `WORKER_OUTPUT_URI_PREFIX`
- `WORKER_STATE_DIR`
- `WORKER_STATE_URI_PREFIX`
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
- `WORKER_STORAGE_ENDPOINT_URL`
- `WORKER_STORAGE_ACCESS_KEY_ID`
- `WORKER_STORAGE_SECRET_ACCESS_KEY`
- `WORKER_STORAGE_REGION`
- `WORKER_STORAGE_FORCE_PATH_STYLE`
- `WORKER_OCR_COMMAND`
- `WORKER_INFERENCE_COMMAND`
- `WORKER_ADAPTER_TIMEOUT_SECONDS`

## Hardening agregado

- Reconexion gRPC con backoff exponencial y `keepalive`.
- Health server listo para Docker y orquestadores.
- Manejo de `SIGINT` y `SIGTERM` para cierre ordenado.
- Persistencia ligera de resultados terminales para deduplicacion tras reinicios sobre storage local o compartido.
- Restauracion automatica de cola pendiente desde disco o `state_uri_prefix`.
- Spool durable de progreso y resultados hasta recibir `ack` del coordinador, con replay tras reinicio.
- Cancelacion cooperativa por defecto para las transformaciones integradas del worker. El aislamiento en proceso dedicado queda reservado para extensiones no cooperativas o cuando una tarea lo pida explicitamente con `metadata.execution_isolation=process`.
- `input_uri` y `output_uri` listos para integrarse con storage compartido sin modificar el contrato gRPC.
- OCR e inferencia integrados via adaptadores por comando (`WORKER_OCR_COMMAND`, `WORKER_INFERENCE_COMMAND` o metadata por tarea).

## Limitaciones residuales

- `http(s)` se soporta como origen de lectura y destino `PUT` para URIs firmadas; para listados durables de estado compartido se recomienda `file://` montado o `s3://`/MinIO.
- La propiedad de cola sigue siendo por nodo. Compartir `state_uri_prefix` mejora durabilidad y observabilidad, pero no convierte al worker en una cola distribuida multi-owner.
- OCR e inferencia dependen de un backend ejecutable configurado por el operador. El worker ya integra el adaptador y el contrato, pero no empaqueta modelos pesados ni binarios externos por defecto.
