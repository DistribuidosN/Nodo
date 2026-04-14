param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "protos", "dev-stack", "dev-down", "worker-stack", "worker-down", "test", "demo", "compare-batch")]
    [string]$Command = "worker-stack"
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

function Invoke-Step([string]$Message, [scriptblock]$Action) {
    Write-Host "==> $Message"
    & $Action
}

switch ($Command) {
    "install" {
        Invoke-Step "Instalando dependencias" { python -m pip install -e .[dev] }
    }
    "protos" {
        Invoke-Step "Generando protos worker_node" { python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/worker_node.proto }
        Invoke-Step "Generando protos imagenode" { python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/imagenode.proto }
    }
    "dev-stack" {
        Invoke-Step "Generando certificados y secretos de desarrollo" { python scripts/dev/generate_dev_security_assets.py }
        Invoke-Step "Levantando entorno local completo" { docker compose -f docker-compose-dev.yml up -d --build }
        Invoke-Step "Mostrando servicios" { docker compose -f docker-compose-dev.yml ps }
    }
    "dev-down" {
        Invoke-Step "Bajando entorno local completo" { docker compose -f docker-compose-dev.yml down }
    }
    "worker-stack" {
        Invoke-Step "Levantando worker individual" { docker compose up -d --build }
        Invoke-Step "Mostrando servicios" { docker compose ps }
    }
    "worker-down" {
        Invoke-Step "Bajando worker individual" { docker compose down }
    }
    "test" {
        Invoke-Step "Ejecutando tests" { python -m pytest }
    }
    "demo" {
        Invoke-Step "Ejecutando demo contra worker1" {
            python scripts/demo/demo_end_to_end.py --target 127.0.0.1:50051 --output-dir docs/demo --ca .secrets/pki/root-ca.crt --cert .secrets/pki/demo-client.crt --key .secrets/pki/demo-client.key --server-name worker.service
        }
    }
    "compare-batch" {
        Invoke-Step "Comparando el mismo batch contra worker1, worker2 y worker3" {
            python examples/compare_workers_batch.py --output-root results --ca .secrets/pki/root-ca.crt --cert .secrets/pki/demo-client.crt --key .secrets/pki/demo-client.key --server-name worker.service
        }
    }
}
