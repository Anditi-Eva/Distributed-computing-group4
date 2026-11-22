# Auto-rebooting supervisor for your Nairobi PC Edge worker (:5001)
# Matches Dublin & Tokyo's 10-second reboot behavior so crashing Edge on the dashboard
# automatically brings your PC back online as a standby replica!
$ErrorActionPreference = "Continue"
Set-Location "C:\dev\ics2403\Distributed-computing-group4\worker_vms"
$env:NODE_ID = "edge"
$env:NODE_PORT = "5001"
$env:NODE_CPU = "8"
$env:NODE_MEMORY = "2048"
$env:BIND_HOST = "127.0.0.1"

while ($true) {
    Write-Host "[Nairobi Edge] Starting worker_node.py on 127.0.0.1:5001 (8 CPU / 2048 MB)..." -ForegroundColor Cyan
    & "..\..\venv\Scripts\python.exe" worker_node.py
    Write-Host "[Nairobi Edge] Crashed or stopped! Auto-rebooting in 10 seconds..." -ForegroundColor Yellow
    Start-Sleep -Seconds 10
}
