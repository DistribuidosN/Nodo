# Distributed Image Worker

Plataforma distribuida en Python para procesamiento de imagenes con gRPC. El repositorio incluye tres nodos worker, un clúster de coordinadores activo/pasivo con failover real, Redis para eleccion de lider, MinIO como storage compartido obligatorio en el despliegue contenedorizado, mTLS extremo a extremo y backends concretos de OCR e inferencia.

## Decisiones principales

- `asyncio + grpc.aio` para control, scheduling y telemetria.
- `Pillow` para la primera version porque cubre `grayscale`, `resize`, `crop`, `rotate`, `flip`, `blur`, `sharpen`, `brightness/contrast`, `watermark/text` y conversion de formato con menor complejidad operativa que OpenCV.
- `ThreadPoolExecutor` para el set integrado de transformaciones cooperativas y procesos dedicados solo para aislamiento explicito o extensiones no cooperativas.
- Cola priorizada con `heapq`, score hibrido por prioridad, deadline, aging, costo estimado y tamano.
- Canal persistente gRPC hacia un coordinador funcional para `ReportProgress`, `ReportResult` y `Heartbeat`.
- `input_uri` y `output_uri/output_uri_prefix` soportados sobre filesystem local, `file://`, `http(s)` de solo lectura y `s3://`/MinIO.
- Persistencia ligera de resultados, cola pendiente y spool durable sobre filesystem local o un backend compartido configurable por URI.
- Clúster de coordinadores activo/pasivo con eleccion de lider por Redis, estado compartido en MinIO y balanceo TCP por HAProxy.
- Coordinador con persistencia durable de cola global, leases de ownership por solicitud, deduplicacion/caching global por fingerprint y failover probado.
- OCR integrado con Tesseract y `pytesseract`; inferencia integrada con un modelo concreto ligero por centroides sobre features visuales.
- Healthchecks HTTP en `/livez` y `/readyz` separados del plano gRPC.
- mTLS obligatorio en el stack Docker, con PKI de desarrollo generada automaticamente por `run.ps1`.

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

Configurar variables locales:

```bash
cp .env.example .env
```

En Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

## Script unico de arranque

El repositorio incluye `run.ps1` para las tareas repetitivas mas comunes:

```powershell
.\run.ps1 install
.\run.ps1 protos
.\run.ps1 stack
.\run.ps1 demo
.\run.ps1 down
.\run.ps1 test
```

Comandos disponibles:

- `install`: instala dependencias del proyecto.
- `protos`: regenera `worker_node.proto` e `imagenode.proto`.
- `stack`: genera PKI/secrets de desarrollo y levanta Redis + MinIO + 2 coordinadores + HAProxy + 3 workers con `docker compose`; si Docker no esta disponible, intenta arranque local como respaldo.
- `demo`: ejecuta una corrida extremo a extremo por TLS contra el coordinador publicado en `coordinator-lb` y guarda artefactos en `docs/demo/`.
- `down`: baja `docker compose` y detiene cualquier proceso local lanzado con `run.ps1`.
- `test`: corre la suite de `pytest`.
- `compose-up` y `compose-down`: controlan el despliegue por Docker.

## Generacion de protos

```bash
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/worker_node.proto
python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/imagenode.proto
```

## Compatibilidad con el modelo de negocio

El worker expone dos servicios gRPC en el mismo puerto:

- `WorkerControlService`: contrato interno del worker para submit, cancelacion, drain, shutdown y estado del nodo.
- `ImageNodeService`: contrato compatible con el modelo de negocio del equipo, con `ProcessToPath`, `ProcessToData`, `UploadLargeImage`, `StreamBatchProcess`, `ProcessBatch`, `HealthCheck`, `GetMetrics` y metodos de busqueda de resultados procesados.

`ImageNodeService` no crea un runtime distinto. Traduce cada `ProcessRequest` al mismo motor interno del worker: cola priorizada, scheduler local, retries, backpressure, persistencia y reporte al coordinador.

Sintaxis de `filters` soportada en `ImageNodeService`:

- `grayscale`
- `resize:640x480`
- `crop:10,20,210,180`
- `rotate:90` o `rotate:90,false`
- `flip:horizontal|vertical|both`
- `blur:2.5`
- `sharpen:1.8`
- `brightness:1.2`
- `contrast:0.9`
- `brightness_contrast:1.1,0.95`
- `watermark:Texto|16|16|white`
- `format:jpg`
- `format:webp`
- `format:bmp`
- `format:gif`
- `format:ico`
- `ocr`
- `inference`

## Ejecutar localmente

Terminal 1:

```bash
python -m coordinator
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

Enviar una solicitud de negocio al coordinador:

```bash
python examples/imagenode_client.py --target 127.0.0.1:50052
```

Prueba simple de carga:

```bash
python examples/load_submitter.py --count 100 --concurrency 16
```

## Contenedores

Levantar stack HA completo:

```bash
docker compose up --build
```

Servicios expuestos:

- Coordinator gRPC publicado por HAProxy: `127.0.0.1:50052`
- Worker 1 gRPC: `127.0.0.1:50051`
- Worker 2 gRPC: `127.0.0.1:50061`
- Worker 3 gRPC: `127.0.0.1:50071`
- MinIO API: `http://127.0.0.1:9000`
- MinIO console: `http://127.0.0.1:9001`
- Worker 1 health: `http://127.0.0.1:8081/readyz`
- Worker 2 health: `http://127.0.0.1:8082/readyz`
- Worker 3 health: `http://127.0.0.1:8083/readyz`
- Worker 1 metrics: `http://127.0.0.1:9101`
- Worker 2 metrics: `http://127.0.0.1:9102`
- Worker 3 metrics: `http://127.0.0.1:9103`

Cada nodo worker es la misma aplicacion desplegada 3 veces con diferente `WORKER_NODE_ID` y puertos externos distintos. El coordinador se ejecuta en dos instancias, comparte estado durable en MinIO, elige lider via Redis y publica un unico endpoint gRPC a traves de HAProxy. Solo el lider queda `ready`; el follower permanece sincronizado y asume el liderazgo si el lider cae.

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

## Como demostrarlo en 5 minutos

1. Copia la configuracion base:

```powershell
Copy-Item .env.example .env
```

2. Instala dependencias y protos:

```powershell
.\run.ps1 install
.\run.ps1 protos
```

3. Levanta el stack seguro:

```powershell
.\run.ps1 stack
```

4. Ejecuta la demo extremo a extremo:

```powershell
.\run.ps1 demo
```

5. Revisa los artefactos generados:

- `docs/demo/demo-input.png`
- `docs/demo/demo-output.png`
- `docs/demo/demo-summary.json`
- `docs/demo/demo-run.txt`

6. Verifica salud y metricas:

- `http://127.0.0.1:8081/readyz`
- `http://127.0.0.1:8082/readyz`
- `http://127.0.0.1:8083/readyz`
- `http://127.0.0.1:9101`
- `http://127.0.0.1:9102`
- `http://127.0.0.1:9103`

7. Prueba failover real:

```powershell
docker compose stop coordinator1
.\run.ps1 demo
docker compose up -d coordinator1
```

La segunda demo debe seguir funcionando y el `HealthCheck` debe reportar el otro `node_id` como lider activo.

8. Deten el stack:

```powershell
.\run.ps1 down
```

La carpeta `docs/demo/` funciona como evidencia reproducible del flujo extremo a extremo.

## Variables de entorno utiles

Coordinador:

- `COORDINATOR_NODE_ID`
- `COORDINATOR_CLUSTER_ID`
- `COORDINATOR_INSTANCE_ID`
- `COORDINATOR_BIND_HOST`
- `COORDINATOR_BIND_PORT`
- `COORDINATOR_HEALTH_HOST`
- `COORDINATOR_HEALTH_PORT`
- `COORDINATOR_STATE_DIR`
- `COORDINATOR_STATE_URI_PREFIX`
- `COORDINATOR_REQUIRE_SHARED_STORAGE`
- `COORDINATOR_WORKERS`
- `COORDINATOR_REDIS_URL`
- `COORDINATOR_LEADER_LOCK_TTL_SECONDS`
- `COORDINATOR_DISPATCH_CONCURRENCY`
- `COORDINATOR_DISPATCH_WAIT_SECONDS`
- `COORDINATOR_STATUS_POLL_SECONDS`
- `COORDINATOR_RPC_TIMEOUT_SECONDS`
- `COORDINATOR_OWNERSHIP_LEASE_SECONDS`
- `COORDINATOR_LOG_LEVEL`
- `COORDINATOR_STORAGE_ENDPOINT_URL`
- `COORDINATOR_STORAGE_ACCESS_KEY_ID`
- `COORDINATOR_STORAGE_SECRET_ACCESS_KEY`
- `COORDINATOR_STORAGE_REGION`
- `COORDINATOR_STORAGE_FORCE_PATH_STYLE`
- `COORDINATOR_GRPC_SERVER_CERT_FILE`
- `COORDINATOR_GRPC_SERVER_KEY_FILE`
- `COORDINATOR_GRPC_SERVER_CLIENT_CA_FILE`
- `COORDINATOR_GRPC_SERVER_REQUIRE_CLIENT_AUTH`
- `COORDINATOR_WORKER_CA_FILE`
- `COORDINATOR_WORKER_CLIENT_CERT_FILE`
- `COORDINATOR_WORKER_CLIENT_KEY_FILE`
- `COORDINATOR_WORKER_SERVER_NAME_OVERRIDE`

- `WORKER_NODE_ID`
- `WORKER_BIND_HOST`
- `WORKER_BIND_PORT`
- `WORKER_COORDINATOR_TARGET`
- `WORKER_OUTPUT_DIR`
- `WORKER_OUTPUT_URI_PREFIX`
- `WORKER_STATE_DIR`
- `WORKER_STATE_URI_PREFIX`
- `WORKER_REQUIRE_SHARED_STORAGE`
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
- `WORKER_TESSERACT_CMD`
- `WORKER_INFERENCE_COMMAND`
- `WORKER_ADAPTER_TIMEOUT_SECONDS`
- `WORKER_GRPC_SERVER_CERT_FILE`
- `WORKER_GRPC_SERVER_KEY_FILE`
- `WORKER_GRPC_SERVER_CLIENT_CA_FILE`
- `WORKER_GRPC_SERVER_REQUIRE_CLIENT_AUTH`
- `WORKER_COORDINATOR_CA_FILE`
- `WORKER_COORDINATOR_CLIENT_CERT_FILE`
- `WORKER_COORDINATOR_CLIENT_KEY_FILE`
- `WORKER_COORDINATOR_SERVER_NAME_OVERRIDE`
- `WORKER_TRACING_ENABLED`
- `WORKER_TRACING_SERVICE_NAME`
- `WORKER_TRACING_OTLP_ENDPOINT`

## Hardening agregado

- Reconexion gRPC con backoff exponencial y `keepalive`.
- Clúster de coordinadores activo/pasivo con Redis para liderazgo, HAProxy para endpoint unico y failover real validado en runtime.
- Coordinador funcional con cola central durable, leases de ownership, deduplicacion global y despacho a workers reales via `ImageNodeService`.
- Health server listo para Docker y orquestadores.
- Manejo de `SIGINT` y `SIGTERM` para cierre ordenado.
- Persistencia ligera de resultados terminales para deduplicacion tras reinicios sobre storage local o compartido.
- Restauracion automatica de cola pendiente desde disco o `state_uri_prefix`.
- Spool durable de progreso y resultados hasta recibir `ack` del coordinador, con replay tras reinicio.
- Cancelacion cooperativa por defecto para las transformaciones integradas del worker. El aislamiento en proceso dedicado queda reservado para extensiones no cooperativas o cuando una tarea lo pida explicitamente con `metadata.execution_isolation=process`.
- Storage compartido obligatorio en el stack Docker sobre MinIO (`s3://distributed-state` y `s3://distributed-output`).
- OCR concreto con Tesseract y `pytesseract`; inferencia concreta con un modelo centroidal ligero incluido en el repo.
- mTLS obligatorio en el stack Docker y opcional en ejecucion local.
- Propagacion de trazas por metadata gRPC y soporte opcional de OpenTelemetry/OTLP.

## CI

La accion de GitHub en `.github/workflows/ci.yml` corre automaticamente en cada `push` y `pull_request`:

- instalacion del proyecto en Python 3.11
- regeneracion de protos
- ejecucion de la suite `pytest`

## Notas operativas

- El despliegue Docker usa coordinadores activo/pasivo; no es un modo activo-activo multi-writer.
- El backend de inferencia incluido es intencionalmente ligero para mantener el runtime portable; puede sustituirse por un modelo mayor sin cambiar el contrato gRPC.
- Si ejecutas OCR fuera de Docker en Windows, instala Tesseract localmente y define `WORKER_TESSERACT_CMD`.
