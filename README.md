# 🚀 Distributed Image Worker (Go + Python Persistent Pool)

Este nodo Worker utiliza una **Arquitectura Híbrida de Altísimo Rendimiento**, combinando la velocidad concurrente y de red robusta de **Go (Golang)** con la flexibilidad para el procesamiento de Visión por Computadora y Machine Learning de **Python**.

---

## 🏗️ Arquitectura del Sistema

El sistema ha evolucionado de usar Python exclusivo, a utilizar **Go como un Orquestador Local** que administra un "Pool" de subprocesos persistentes de Python.

```mermaid
graph TD
    subgraph Servidor Java ["Quarkus Orquestador Principal"]
        Java["Backend Central"]
    end

    subgraph Go Agent ["Nodo Worker (Go)"]
        WorkStealer["WorkStealing Poller (ConnectRPC HTTP/2)"]
        Pool["Worker Pool Manager"]
        IPC["JSON-RPC IPC Pipe (stdin/stdout)"]
    end

    subgraph Python ["Capa de Procesamiento (Python 3.11)"]
        Worker1["Subproceso Python 1 (Pesistente)"]
        Worker2["Subproceso Python N (Pesistente)"]
        Models[("Modelos de OCR e Inferencia Globales")]
    end

    Java <==>|PULL Tareas (HTTP/2)| WorkStealer
    WorkStealer --> Pool
    Pool ==> IPC
    IPC <==>|JSON| Worker1
    IPC <==>|JSON| Worker2
    Worker1 --> Models
```

### 🧠 Modelo de Trabajo: Work-Stealing Rápido IPC
1. **Go Agent** se conecta por gRPC directo vía HTTP/2 al Orquestador en Java y hace un "PULL", preguntando si hay tareas gráficas (según sus CPU disponibles).
2. Go recibe y balancea las tareas hacia su local **Worker Pool**.
3. El **Python Worker** es un hilo que nunca muere. Carga los pesados modelos de IA una sola vez, recibe las imágenes e intrucciones JSON vía `stdin`, hace su magia (*Tesseract, Centroides de Imagen*), devuelve la respuesta JSON vía `stdout` y se auto-resetea esperando la siguiente tarea a la velocidad de la luz.

---

## 🛠️ Instalación y Requisitos

### 1. Requisitos para Python (Lógica de Modelos)
1. Instalar **Python 3.11+**.
2. Instalar el ejecutable del motor **Tesseract OCR** en Windows desde el [Link Oficial](https://github.com/UB-Mannheim/tesseract/wiki).
3. Instalar dependencias globales:
   ```powershell
   pip install Pillow pytesseract numpy
   ```

### 2. Requisitos para Go (Orquestación Local de Red)
1. Descarga e instala Go desde su sitio oficial [golang.org/dl](https://golang.org/dl/).
2. Comprueba tu instalación (requiere reiniciar PowerShell después de instalar):
   ```powershell
   go version
   ```

---

## 🚀 Cómo Correr el Sistema

### Ejecución Directa (Desarrollo)
Entra a la carpeta de Go y lánzalo nativamente. El orquestador Go activará automáticamente a los bots de Python por detrás:
```powershell
cd Nodo/orchestrator
go run .\server\main.go
```

---

## 🏗️ Builds para Producción (Windows y Linux)

Go permite empaquetar todo el servidor Go en un ejecutable ultra rápido (sin necesidad de instalar Go en la máquina que lo usará). 

*Abre una terminal y dirígete a `Nodo/orchestrator/`.*

### 🪟 1. Compilar para Windows (.exe)
```powershell
go build -o node_agent.exe .\server\main.go
```
Para ejecutarlo, haz doble click o cópialo, y lánzalo:
```powershell
.\node_agent.exe
```

### 🐧 2. Compilar para Servidores Linux 
Puedes compilar la versión de Linux sin necesidad de estar usando Linux. En tu misma consola PowerShell ajusta lo siguiente:
```powershell
$env:GOOS="linux"
$env:GOARCH="amd64"
go build -o node_agent_linux .\server\main.go
```
Para ejecutarlo en la VPS Linux, recuerda que la máquina destino solo necesita tener Python y Tesseract instalados; subes el ejecutable, le añades permisos y lo lanzas:
```bash
chmod +x node_agent_linux
./node_agent_linux
```

---

## ⚙️ Configuración del Entorno (.env)

El archivo maestro de variables se ubica en `orchestrator/.env`:

| Variable | Descripción |
| :--- | :--- |
| `JAVA_ORCHESTRATOR_URL` | La URL del servidor Quarkus en Java (ej: `http://localhost:9000`). |
| `LOCAL_GRPC_PORT` | Puerto servidor Go para futuras implementaciones (ej: `:50051`). |
| `PYTHON_SCRIPT` | Ruta en disco del worker (`../worker/worker.py`). |
| `WORKER_TESSERACT_CMD` | Ruta absoluta binario OCR (ej: `C:\Program Files\Tesseract-OCR\tesseract.exe`). Si es errónea, el worker aplicará "Fallback" modo Simulacro. |
| `NODE_ID` | Nombre opcional del Nodo. Si está vacío, se auto-genera aleatoriamente cada que enciendas el sistema. |
