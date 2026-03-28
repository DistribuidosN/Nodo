param(
    [ValidateSet(
        "help",
        "install",
        "protos",
        "test",
        "coordinator",
        "worker1",
        "worker2",
        "worker3",
        "stack",
        "down",
        "demo",
        "compose-up",
        "compose-down"
    )]
    [string]$Command = "help"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $Root

function Load-DotEnv {
    param([string]$Path)
    if (-not (Test-Path $Path)) {
        return
    }
    Get-Content $Path | ForEach-Object {
        $line = $_.Trim()
        if (-not $line -or $line.StartsWith("#")) {
            return
        }
        $name, $value = $line -split "=", 2
        if (-not $name -or $null -eq $value) {
            return
        }
        $value = $value.Trim()
        if (($value.StartsWith('"') -and $value.EndsWith('"')) -or ($value.StartsWith("'") -and $value.EndsWith("'"))) {
            $value = $value.Substring(1, $value.Length - 2)
        }
        [System.Environment]::SetEnvironmentVariable($name.Trim(), $value)
    }
}

function Ensure-DevSecurityAssets {
    & python scripts/generate_dev_security_assets.py
}

function Invoke-PythonModule {
    param(
        [string]$Module,
        [hashtable]$Environment = @{}
    )
    $previous = @{}
    foreach ($key in $Environment.Keys) {
        $previous[$key] = [System.Environment]::GetEnvironmentVariable($key)
        [System.Environment]::SetEnvironmentVariable($key, [string]$Environment[$key])
    }
    try {
        & python -m $Module
    }
    finally {
        foreach ($key in $Environment.Keys) {
            [System.Environment]::SetEnvironmentVariable($key, $previous[$key])
        }
    }
}

function Start-BackgroundProcess {
    param(
        [string]$Name,
        [string]$Module,
        [hashtable]$Environment = @{}
    )

    $runDir = Join-Path $Root ".run"
    New-Item -ItemType Directory -Force -Path $runDir | Out-Null
    $stdout = Join-Path $runDir "$Name.out.log"
    $stderr = Join-Path $runDir "$Name.err.log"
    $pidFile = Join-Path $runDir "$Name.pid"

    if (Test-Path $pidFile) {
        $existingPid = Get-Content $pidFile -ErrorAction SilentlyContinue
        if ($existingPid) {
            $existingProcess = Get-Process -Id ([int]$existingPid) -ErrorAction SilentlyContinue
            if ($existingProcess) {
                Write-Host "$Name ya esta corriendo con PID $existingPid"
                return
            }
        }
        Remove-Item $pidFile -Force -ErrorAction SilentlyContinue
    }

    $previous = @{}
    foreach ($key in $Environment.Keys) {
        $previous[$key] = [System.Environment]::GetEnvironmentVariable($key)
        [System.Environment]::SetEnvironmentVariable($key, [string]$Environment[$key])
    }
    try {
        $process = Start-Process python `
            -ArgumentList @("-m", $Module) `
            -WorkingDirectory $Root `
            -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr `
            -PassThru
        $process.Id | Set-Content $pidFile
        Write-Host "Iniciado $Name con PID $($process.Id)"
    }
    finally {
        foreach ($key in $Environment.Keys) {
            [System.Environment]::SetEnvironmentVariable($key, $previous[$key])
        }
    }
}

function Stop-BackgroundProcesses {
    $runDir = Join-Path $Root ".run"
    if (-not (Test-Path $runDir)) {
        return
    }
    Get-ChildItem $runDir -Filter *.pid | ForEach-Object {
        $pidValue = Get-Content $_.FullName -ErrorAction SilentlyContinue
        if (-not $pidValue) {
            Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
            return
        }
        $process = Get-Process -Id ([int]$pidValue) -ErrorAction SilentlyContinue
        if ($process) {
            Stop-Process -Id $process.Id -Force
            Write-Host "Detenido $($_.BaseName) con PID $pidValue"
        }
        Remove-Item $_.FullName -Force -ErrorAction SilentlyContinue
    }
}

function Start-LocalStack {
    Stop-BackgroundProcesses
    Start-BackgroundProcess -Name "coordinator" -Module "coordinator" -Environment @{
        "COORDINATOR_NODE_ID" = "coordinator-main"
        "COORDINATOR_BIND_HOST" = "127.0.0.1"
        "COORDINATOR_BIND_PORT" = "50052"
        "COORDINATOR_STATE_DIR" = "data/coordinator/state"
        "COORDINATOR_WORKERS" = "worker-1=127.0.0.1:50051,worker-2=127.0.0.1:50061,worker-3=127.0.0.1:50071"
    }
    Start-BackgroundProcess -Name "worker1" -Module "worker" -Environment @{
        "WORKER_NODE_ID" = "worker-1"
        "WORKER_BIND_HOST" = "127.0.0.1"
        "WORKER_BIND_PORT" = "50051"
        "WORKER_COORDINATOR_TARGET" = "127.0.0.1:50052"
        "WORKER_METRICS_HOST" = "127.0.0.1"
        "WORKER_METRICS_PORT" = "9101"
        "WORKER_HEALTH_HOST" = "127.0.0.1"
        "WORKER_HEALTH_PORT" = "8081"
        "WORKER_OUTPUT_DIR" = "data/worker1/out"
        "WORKER_STATE_DIR" = "data/worker1/state"
    }
    Start-BackgroundProcess -Name "worker2" -Module "worker" -Environment @{
        "WORKER_NODE_ID" = "worker-2"
        "WORKER_BIND_HOST" = "127.0.0.1"
        "WORKER_BIND_PORT" = "50061"
        "WORKER_COORDINATOR_TARGET" = "127.0.0.1:50052"
        "WORKER_METRICS_HOST" = "127.0.0.1"
        "WORKER_METRICS_PORT" = "9102"
        "WORKER_HEALTH_HOST" = "127.0.0.1"
        "WORKER_HEALTH_PORT" = "8082"
        "WORKER_OUTPUT_DIR" = "data/worker2/out"
        "WORKER_STATE_DIR" = "data/worker2/state"
    }
    Start-BackgroundProcess -Name "worker3" -Module "worker" -Environment @{
        "WORKER_NODE_ID" = "worker-3"
        "WORKER_BIND_HOST" = "127.0.0.1"
        "WORKER_BIND_PORT" = "50071"
        "WORKER_COORDINATOR_TARGET" = "127.0.0.1:50052"
        "WORKER_METRICS_HOST" = "127.0.0.1"
        "WORKER_METRICS_PORT" = "9103"
        "WORKER_HEALTH_HOST" = "127.0.0.1"
        "WORKER_HEALTH_PORT" = "8083"
        "WORKER_OUTPUT_DIR" = "data/worker3/out"
        "WORKER_STATE_DIR" = "data/worker3/state"
    }
    Start-Sleep -Seconds 6
    Write-Host "Stack local iniciado. Logs en .run/"
}

Load-DotEnv (Join-Path $Root ".env")

switch ($Command) {
    "help" {
        Write-Host "Uso: .\run.ps1 [install|protos|test|coordinator|worker1|worker2|worker3|stack|down|demo|compose-up|compose-down]"
    }
    "install" {
        & python -m pip install -e ".[dev]"
    }
    "protos" {
        & python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/worker_node.proto
        & python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. proto/imagenode.proto
    }
    "test" {
        & python -m pytest
    }
    "coordinator" {
        Invoke-PythonModule -Module "coordinator"
    }
    "worker1" {
        Invoke-PythonModule -Module "worker" -Environment @{
            "WORKER_NODE_ID" = "worker-1"
            "WORKER_BIND_PORT" = "50051"
            "WORKER_COORDINATOR_TARGET" = "127.0.0.1:50052"
            "WORKER_METRICS_PORT" = "9101"
            "WORKER_HEALTH_PORT" = "8081"
            "WORKER_OUTPUT_DIR" = "data/worker1/out"
            "WORKER_STATE_DIR" = "data/worker1/state"
        }
    }
    "worker2" {
        Invoke-PythonModule -Module "worker" -Environment @{
            "WORKER_NODE_ID" = "worker-2"
            "WORKER_BIND_PORT" = "50061"
            "WORKER_COORDINATOR_TARGET" = "127.0.0.1:50052"
            "WORKER_METRICS_PORT" = "9102"
            "WORKER_HEALTH_PORT" = "8082"
            "WORKER_OUTPUT_DIR" = "data/worker2/out"
            "WORKER_STATE_DIR" = "data/worker2/state"
        }
    }
    "worker3" {
        Invoke-PythonModule -Module "worker" -Environment @{
            "WORKER_NODE_ID" = "worker-3"
            "WORKER_BIND_PORT" = "50071"
            "WORKER_COORDINATOR_TARGET" = "127.0.0.1:50052"
            "WORKER_METRICS_PORT" = "9103"
            "WORKER_HEALTH_PORT" = "8083"
            "WORKER_OUTPUT_DIR" = "data/worker3/out"
            "WORKER_STATE_DIR" = "data/worker3/state"
        }
    }
    "stack" {
        Ensure-DevSecurityAssets
        try {
            & docker compose up -d --build
        }
        catch {
            Write-Warning "docker compose no estuvo disponible; usando arranque local"
            Start-LocalStack
        }
    }
    "down" {
        try {
            & docker compose down
        }
        catch {
        }
        Stop-BackgroundProcesses
    }
    "demo" {
        Ensure-DevSecurityAssets
        & python scripts/demo_end_to_end.py `
            --target 127.0.0.1:50052 `
            --output-dir docs/demo `
            --ca .secrets/pki/root-ca.crt `
            --cert .secrets/pki/demo-client.crt `
            --key .secrets/pki/demo-client.key `
            --server-name coordinator.service
    }
    "compose-up" {
        Ensure-DevSecurityAssets
        & docker compose up --build
    }
    "compose-down" {
        & docker compose down
    }
}
