# Distributed Image Workers

Subsistema de nodos worker para procesamiento distribuido de imágenes en Python con `asyncio + grpc.aio`.

Este repositorio ya no despliega coordinador propio. La arquitectura objetivo es:

`Cliente -> Servidor principal Java -> Workers Python`

El servidor principal en Java se encarga de autenticación, base de datos, reglas de negocio y coordinación global. Este repo contiene únicamente:

- `3` workers listos para exponer gRPC.
- interfaces protobuf para que el servidor Java se conecte.
- cola local con prioridad, scheduling interno, backpressure y retries.
- OCR e inferencia concretos dentro del worker.
- storage compartido opcional con MinIO/S3.
- métricas, healthchecks, Prometheus y Grafana.

## Arquitectura

Cada worker hace esto:

- recibe tareas por gRPC;
- valida tamaño, dimensiones, filtros y lotes;
- encola localmente con prioridad;
- decide cuándo ejecutar según recursos;
- procesa la imagen en paralelo;
- persiste resultados/estado;
- opcionalmente reporta progreso, heartbeat y resultados al servidor principal externo si este implementa el callback gRPC.

## Contratos gRPC

### 1. Servicio de negocio expuesto por cada worker

Archivo: [`proto/imagenode.proto`](c:/Users/acero/Downloads/nodos/Nodo/proto/imagenode.proto)

Servicio:

- `ImageNodeService`

RPCs principales:

- `ProcessToPath`
- `ProcessToData`
- `UploadLargeImage`
- `StreamBatchProcess`
- `ProcessBatch`
- `HealthCheck`
- `GetMetrics`
- `GetProcessedFilePaths`
- `FindPathByName`
- `GetProcessedImages`
- `FindImageByName`

Este es el contrato más natural para que el servidor principal Java invoque procesamiento directamente.

### 2. Servicio de control expuesto por cada worker

Archivo: [`proto/worker_node.proto`](c:/Users/acero/Downloads/nodos/Nodo/proto/worker_node.proto)

Servicio:

- `WorkerControlService`

RPCs principales:

- `SubmitTask`
- `GetNodeStatus`
- `CancelTask`
- `DrainNode`
- `ShutdownNode`

Este contrato sirve si el servidor principal quiere controlar cola, estado y ciclo de vida del worker de forma explícita.

### 3. Callback opcional que debe implementar el servidor principal

Archivo: [`proto/worker_node.proto`](c:/Users/acero/Downloads/nodos/Nodo/proto/worker_node.proto)

Servicio:

- `CoordinatorCallbackService`

RPCs:

- `ReportProgress`
- `ReportResult`
- `Heartbeat`

Si configuras `WORKER_COORDINATOR_TARGET`, el worker enviará estos callbacks al servidor principal. Si lo dejas vacío, el worker funciona en modo standalone.

## Funcionalidad del worker

Transformaciones soportadas:

- `grayscale`
- `resize`
- `crop`
- `rotate`
- `flip`
- `blur`
- `sharpen`
- `brightness/contrast`
- `watermark/text`
- conversión de formato: `jpg`, `png`, `tif`, `webp`, `bmp`, `gif`, `ico`
- `ocr`
- `inference`

El worker ya incluye:

- validación estricta de entrada;
- persistencia de pendientes y resultados;
- deduplicación local;
- cancelación cooperativa;
- backpressure con HWM/LWM;
- métricas Prometheus;
- healthchecks `/livez` y `/readyz`;
- mTLS opcional.

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
examples/
tests/
```

## Instalación

```bash
python -m pip install -e .[dev]
```

En Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

## Variables importantes

### Integración con el servidor principal

- `APP_SERVER_GRPC_TARGET`
- `APP_SERVER_SERVER_NAME_OVERRIDE`
- `WORKER_COORDINATOR_TARGET`
- `WORKER_COORDINATOR_CA_FILE`
- `WORKER_COORDINATOR_CLIENT_CERT_FILE`
- `WORKER_COORDINATOR_CLIENT_KEY_FILE`

Si `WORKER_COORDINATOR_TARGET` está vacío, el worker no intenta reportar callbacks.

### Límites de entrada

- `WORKER_MAX_REQUEST_BYTES`
- `WORKER_MAX_BATCH_SIZE`
- `WORKER_MAX_FILTERS_PER_REQUEST`
- `WORKER_MAX_IMAGE_WIDTH`
- `WORKER_MAX_IMAGE_HEIGHT`
- `WORKER_MAX_IMAGE_PIXELS`

### Storage

- `WORKER_OUTPUT_URI_PREFIX`
- `WORKER_STATE_URI_PREFIX`
- `WORKER_REQUIRE_SHARED_STORAGE`
- `WORKER_STORAGE_ENDPOINT_URL`
- `WORKER_STORAGE_ACCESS_KEY_ID`
- `WORKER_STORAGE_SECRET_ACCESS_KEY`
- `WORKER_STORAGE_REGION`
- `WORKER_STORAGE_FORCE_PATH_STYLE`

## Levantar el stack Docker de workers

Este `docker-compose` levanta:

- `minio`
- `worker1`
- `worker2`
- `worker3`
- `prometheus`
- `grafana`

Comando:

```bash
docker compose up --build
```

Puertos:

- Worker 1 gRPC: `127.0.0.1:50051`
- Worker 2 gRPC: `127.0.0.1:50061`
- Worker 3 gRPC: `127.0.0.1:50071`
- Worker 1 health: `http://127.0.0.1:8081/readyz`
- Worker 2 health: `http://127.0.0.1:8082/readyz`
- Worker 3 health: `http://127.0.0.1:8083/readyz`
- Worker 1 metrics: `http://127.0.0.1:9101`
- Worker 2 metrics: `http://127.0.0.1:9102`
- Worker 3 metrics: `http://127.0.0.1:9103`
- MinIO API: `http://127.0.0.1:9000`
- MinIO Console: `http://127.0.0.1:9001`
- Prometheus: `http://127.0.0.1:9090`
- Grafana: `http://127.0.0.1:3000`

Grafana usa por defecto:

- usuario: `admin`
- clave: `admin`

Puedes cambiarlo con:

- `GRAFANA_ADMIN_USER`
- `GRAFANA_ADMIN_PASSWORD`

## Ejecución local simple

Worker:

```bash
python -m worker
```

Enviar una tarea de control a un worker:

```bash
python examples/submit_task.py --target 127.0.0.1:50051
```

Enviar una solicitud de negocio:

```bash
python examples/imagenode_client.py --target 127.0.0.1:50051
```

Enviar una imagen real:

```bash
python examples/send_real_image.py --file "C:\ruta\imagen.png" --target 127.0.0.1:50051 --filter grayscale
```

Prueba de carga:

```bash
python examples/load_submitter.py --target 127.0.0.1:50051 --count 50 --concurrency 8
```

Demo extremo a extremo sobre un worker:

```bash
python scripts/demo_end_to_end.py --target 127.0.0.1:50051 --output-dir docs/demo
```

## Observabilidad

Métricas relevantes:

- `worker_queue_length`
- `worker_report_queue_length`
- `worker_processing_time_ms`
- `worker_success_total`
- `worker_failure_total`
- `worker_retry_total`
- `worker_tasks_rejected_total`
- `worker_tasks_cancelled_total`
- `worker_cpu_utilization_ratio`
- `worker_memory_utilization_ratio`
- `worker_active_tasks`
- `worker_capacity_effective`
- `worker_coordinator_connected`
- `worker_readiness`

Prometheus scrapea automáticamente los tres workers y Grafana provisiona el dashboard `Distributed Image Worker`.

## Seguridad

El stack Docker usa mTLS en el servidor gRPC del worker.

Archivos de desarrollo:

- `.secrets/pki/root-ca.crt`
- `.secrets/pki/worker-server.crt`
- `.secrets/pki/worker-server.key`
- `.secrets/pki/worker-client.crt`
- `.secrets/pki/worker-client.key`

Si tu servidor principal Java va a consumir los workers por mTLS, debe confiar en la CA correcta y, si aplica, presentar certificado cliente válido.

## Tests

```bash
python -m pytest
```

## Estado del proyecto

Este repositorio queda alineado a la arquitectura final:

- el backend principal Java coordina;
- los workers Python ejecutan;
- este repo no asume autenticación de usuario, base de datos de negocio ni orquestación global propia.

El único componente de coordinación que queda aquí es el ejemplo [`examples/mock_app_server.py`](c:/Users/acero/Downloads/nodos/Nodo/examples/mock_app_server.py), pensado solo para pruebas locales del contrato de callbacks.
