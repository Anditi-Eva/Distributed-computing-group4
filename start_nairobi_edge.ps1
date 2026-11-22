# Edge worker on this PC (:5001), restarted 10 s after a crash like the Dublin and Tokyo workers.
# The gateway in Dublin reaches it through a reverse SSH tunnel from this PC:
#   ssh -i <dublin-key.pem> -N -R 5001:127.0.0.1:5001 ubuntu@<dublin-ip>
# Run from the repository root:  powershell -ExecutionPolicy Bypass -File .\start_nairobi_edge.ps1
$ErrorActionPreference = "Continue"
$root = $PSScriptRoot
$python = if ($env:PYTHON) { $env:PYTHON } elseif (Test-Path "$root\..\venv\Scripts\python.exe") { "$root\..\venv\Scripts\python.exe" } else { "python" }
$state = Join-Path $root ".edge_state"
New-Item -ItemType Directory -Force $state | Out-Null

$env:NODE_ID = "edge"
$env:NODE_PORT = "5001"
$env:NODE_CPU = "8"
$env:NODE_MEMORY = "2048"
$env:PEER_HOST = "127.0.0.1"
$env:PEER_PORT = "9"
$env:PROM_PORT = "9101"
$env:WORKER_BIND_HOST = "127.0.0.1"
$env:KAFKA_BOOTSTRAP_SERVERS = ""
$env:SHARED_STATE_DB = Join-Path $state "edge-state.sqlite3"
$env:FILE_STORE_DB = Join-Path $state "edge-files.sqlite3"

Set-Location (Join-Path $root "worker_vms")
while ($true) {
    Write-Host "[edge] starting worker on 127.0.0.1:5001"
    & $python worker_node.py
    Write-Host "[edge] worker stopped; restarting in 10 s"
    Start-Sleep -Seconds 10
}
