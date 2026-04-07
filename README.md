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

Cada worker levanta un servidor gRPC con dos contratos:

- `imagenode.ImageNodeService`
  Contrato de negocio para pedir procesamiento de imagenes.

- `worker.WorkerControlService`
  Contrato de control del nodo para consultar estado, enviar tareas, cancelar, drenar o apagar.

Protos:

- [proto/imagenode.proto](proto/imagenode.proto)
- [proto/worker_node.proto](proto/worker_node.proto)

## Estructura recomendada para leer el proyecto

Empieza por estos archivos:

1. [worker/server.py](worker/server.py)
2. [worker/core/worker_runtime.py](worker/core/worker_runtime.py)
3. [worker/grpc/image_node_service.py](worker/grpc/image_node_service.py)
4. [worker/grpc/worker_control_service.py](worker/grpc/worker_control_service.py)
5. [worker/execution](worker/execution)

Arbol principal:

```text
worker/
  server.py
  config.py
  ARCHITECTURE.md
  core/
  execution/
  grpc/
  models/
  scheduler/
  telemetry/
proto/
examples/
tests/
```

Los archivos con nombres viejos se mantienen por compatibilidad, pero los
modulos anteriores son los nombres canonicos para leer y mantener el proyecto.

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

### 2. Desarrollo local: tres workers y herramientas auxiliares

Archivo:

- [docker-compose-dev.yml](docker-compose-dev.yml)

Este modo levanta:

- `worker1`
- `worker2`
- `worker3`
- `minio`
- `prometheus`
- `grafana`

Uso:

```powershell
python scripts/generate_dev_security_assets.py
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

### Enviar una tarea al contrato de control

```powershell
python examples/submit_task.py --target 127.0.0.1:50051
```

### Enviar una imagen real al contrato de negocio

```powershell
python examples/send_real_image.py --file "C:\ruta\imagen.png" --target 127.0.0.1:50051 --filter grayscale
```

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
guardar historicos y construir graficas debe quedar del lado del servidor
principal.

## Scripts utiles

- [scripts/demo_end_to_end.py](scripts/demo_end_to_end.py)
- [scripts/generate_dev_security_assets.py](scripts/generate_dev_security_assets.py)
- [scripts/healthcheck.py](scripts/healthcheck.py)
- [scripts/ocr_backend.py](scripts/ocr_backend.py)
- [scripts/inference_backend.py](scripts/inference_backend.py)

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
- dejar la coordinacion real en el servidor principal Java
- mantener un entorno local separado para pruebas con varios workers
