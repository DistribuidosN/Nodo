# Distributed Image Workers

Repositorio de workers en Python para procesamiento de imagenes por gRPC.

Arquitectura final del sistema:

```text
Cliente -> Servidor principal Java -> Workers Python
```

El servidor principal en Java se encarga de:
- autenticacion
- base de datos
- logica de negocio
- decision de a que worker enviar cada tarea
- metricas historicas
- graficas y paneles

Este repositorio deja listo solo el lado de los workers.

## Que expone cada worker

Cada worker levanta un servidor gRPC con tres contratos:

- `imagenode.ImageNodeService`
  Contrato de negocio para pedir procesamiento de imagenes.

- `worker.WorkerControlService`
  Contrato de control del nodo para consultar estado, enviar tareas, cancelar, drenar o apagar.

- `worker.WorkerNode`
  Contrato esperado por el servidor principal Java para consultar metricas del nodo y pedir cesion de tareas.

El servidor principal Java expone:

- `worker.Orchestrator`
  Contrato de coordinacion para `PullTasks`, `SubmitResult`, `UpdateTaskProgress` y `SendHeartbeat`.

Protos:

- [proto/imagenode.proto](proto/imagenode.proto)
- [proto/worker_node.proto](proto/worker_node.proto)
- [proto/orchestrator.proto](proto/orchestrator.proto)

## Estructura recomendada para leer el proyecto

Empieza por estos archivos:

1. [worker/server.py](worker/server.py)
2. [worker/core/worker_runtime.py](worker/core/worker_runtime.py)
3. [worker/grpc/image_node_service.py](worker/grpc/image_node_service.py)
4. [worker/grpc/worker_control_service.py](worker/grpc/worker_control_service.py)
5. [worker/execution](worker/execution)

Arbol principal que conviene conservar:

```text
worker/
proto/
scripts/
tests/
docs/
  api/
  reports/
```

Los archivos con nombres viejos se mantienen por compatibilidad, pero los
modulos anteriores son los nombres canonicos para leer y mantener el proyecto.

Carpetas que se generan localmente y se pueden borrar sin afectar el codigo:

- `data/`
- `results/`
- `output/`
- `tmp/`
- `.run/`
- `.secrets/`
- `.pytest_cache/`
- `.pytest_tmp/`

Artefactos de demo generados:

- `docs/demo/demo-*`

## Modos de despliegue

### 1. Produccion: un worker por maquina o VM

Archivo:

- [docker-compose.yml](docker-compose.yml)

Este modo levanta solo:

- `worker`

Puertos por defecto:

- gRPC: `127.0.0.1:50051`
- Health: `http://127.0.0.1:8081/readyz`
- Metrics: `http://127.0.0.1:9100/metrics`

Uso:

```powershell
docker compose up -d --build
docker compose ps
```

Para apagar:

```powershell
docker compose down
```

### 2. Desarrollo local: tres workers locales

Archivo:

- [docker-compose-dev.yml](docker-compose-dev.yml)

Este modo levanta:

- `worker1`
- `worker2`
- `worker3`

Uso:

```powershell
python scripts/dev/generate_dev_security_assets.py
docker compose -f docker-compose-dev.yml up -d --build
docker compose -f docker-compose-dev.yml ps
```

Para apagar:

```powershell
docker compose -f docker-compose-dev.yml down
```

## Configuracion

### Produccion

Plantilla:

- [.env.production.example](.env.production.example)

Variables importantes:

- `WORKER_NODE_ID`
- `WORKER_BIND_HOST`
- `WORKER_BIND_PORT`
- `WORKER_COORDINATOR_TARGET`
- `WORKER_INPUT_DIR`
- `WORKER_OUTPUT_DIR`
- `WORKER_STATE_DIR`
- `WORKER_METRICS_PORT`
- `WORKER_HEALTH_PORT`

### Desarrollo local

Plantilla:

- [.env.example](.env.example)

## Correr un worker sin Docker

Instalar dependencias:

```powershell
python -m pip install -e .[dev]
```

Levantar el worker:

```powershell
python -m worker
```

Entrada equivalente explicita:

```powershell
python -m worker.server
```

## Probar el worker

Para pruebas manuales del contrato usa:

- la coleccion de Postman en `docs/api/postman/`
- el script [scripts/demo/demo_end_to_end.py](scripts/demo/demo_end_to_end.py)
- o el cliente real del servidor principal Java

### Ver salud

En navegador:

- `http://127.0.0.1:8081/livez`
- `http://127.0.0.1:8081/readyz`

O en terminal:

```powershell
curl http://127.0.0.1:8081/readyz
```

## Como se integra el servidor principal Java

Flujo esperado:

1. El servidor Java recibe la solicitud del cliente.
2. Consulta que workers estan disponibles.
3. Elige el worker mas conveniente.
4. Llama por gRPC:
   - `ImageNodeService` para procesamiento de negocio
   - o `WorkerControlService` para control fino del nodo
5. Recibe el resultado.
6. Guarda estado, metricas historicas y graficas en su propia BD.

En corto:

- Java decide y coordina.
- Python procesa.

## Metricas y health

El worker puede exponer:

- `livez`
- `readyz`
- `metrics`

Eso es util para operacion, pruebas o integracion, pero la responsabilidad de
guardar historicos, persistir metricas y construir graficas debe quedar del
lado del servidor principal o su storage/BD.

## Costo heuristico por filtro para la cola

La cola local penaliza tareas mas costosas usando un peso base por filtro.
Ese valor no reemplaza la prioridad funcional, pero si ayuda a ordenar mejor
la ejecucion cuando entran trabajos heterogeneos.

Valores base actuales:

- `grayscale`: `0.35`
- `resize`: `0.95`
- `crop`: `0.25`
- `rotate`: `0.65`
- `flip`: `0.20`
- `blur`: `1.30`
- `sharpen`: `1.00`
- `brightness` / `contrast` / `brightness_contrast`: `0.60`
- `watermark_text`: `1.10`
- `format`: `0.30`
- `ocr`: `3.20`
- `inference`: `3.80`

Valores explicitos para conversion por formato destino:

- `format:jpg` / `format:jpeg`: `0.300`
- `format:bmp`: `0.255`
- `format:png`: `0.315`
- `format:tif` / `format:tiff`: `0.345`
- `format:ico`: `0.360`
- `format:webp`: `0.375`
- `format:gif`: `0.405`

Ajustes dinamicos importantes:

- `resize` sube mas si hace upscale y baja un poco si hace downscale.
- `rotate` cuesta menos en `90/180/270` que en angulos arbitrarios.
- `blur`, `sharpen` y `brightness_contrast` suben segun intensidad.
- `watermark_text` sube segun tamano y longitud del texto.
- `format` sube para formatos mas pesados como `webp`, `gif` o `ico`.

## Storage local del nodo

Cada worker materializa las entradas y salidas en disco local:

- `data/input`
- `data/output`
- `data/state`

En Docker esos paths viven dentro de `/app/data`.

El worker no necesita MinIO ni storage compartido para operar. Si el servidor
principal quiere centralizar resultados, debe copiarlos o persistirlos desde su
propio lado despues de recibir el reporte o el resultado.

## Scripts utiles

- [scripts/demo/demo_end_to_end.py](scripts/demo/demo_end_to_end.py)
- [scripts/dev/generate_dev_security_assets.py](scripts/dev/generate_dev_security_assets.py)
- [scripts/ops/healthcheck.py](scripts/ops/healthcheck.py)
- [scripts/backends/ocr_backend.py](scripts/backends/ocr_backend.py)
- [scripts/backends/inference_backend.py](scripts/backends/inference_backend.py)

## Comandos rapidos con PowerShell

- `.\run.ps1 worker-stack`
  Levanta un worker individual.

- `.\run.ps1 worker-down`
  Baja el worker individual.

- `.\run.ps1 dev-stack`
  Levanta el entorno local completo.

- `.\run.ps1 dev-down`
  Baja el entorno local completo.

- `.\run.ps1 test`
  Ejecuta los tests.

## Estado final

El proyecto queda preparado para:

- desplegar un worker por maquina o VM
- usar storage local por nodo para entradas, salidas y estado
- dejar la coordinacion real en el servidor principal Java
- mantener un entorno local separado para pruebas con varios workers
