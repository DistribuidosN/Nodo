param(
    [Parameter(Position = 0)]
    [ValidateSet("install", "protos", "stack", "down", "test", "demo")]
    [string]$Command = "stack"
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
    "stack" {
        Invoke-Step "Generando certificados y secretos de desarrollo" { python scripts/generate_dev_security_assets.py }
        Invoke-Step "Levantando stack de workers" { docker compose up -d --build }
        Invoke-Step "Mostrando servicios" { docker compose ps }
    }
    "down" {
        Invoke-Step "Bajando stack" { docker compose down }
    }
    "test" {
        Invoke-Step "Ejecutando tests" { python -m pytest }
    }
    "demo" {
        Invoke-Step "Ejecutando demo contra worker1" {
            python scripts/demo_end_to_end.py --target 127.0.0.1:50051 --output-dir docs/demo --ca .secrets/pki/root-ca.crt --cert .secrets/pki/demo-client.crt --key .secrets/pki/demo-client.key --server-name worker.service
        }
    }
}
