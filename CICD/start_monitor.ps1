# Startup Script for GroteskBotTg Monitor
# This script ensures the monitor runs with proper permissions and error handling

$SCRIPT_DIR = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$MONITOR_SCRIPT = Join-Path $SCRIPT_DIR "CICD\monitor_and_update.ps1"
$ERROR_LOG = Join-Path $SCRIPT_DIR "startup_error.log"

function Write-ErrorLog {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $ERROR_LOG -Value "[$timestamp] $Message"
    Write-Host $Message -ForegroundColor Red
}

try {
    # Check if monitor script exists
    if (-not (Test-Path $MONITOR_SCRIPT)) {
        Write-ErrorLog "Monitor script not found: $MONITOR_SCRIPT"
        exit 1
    }

    Write-Host "Starting GroteskBotTg Monitor..." -ForegroundColor Green
    Write-Host "Log files will be created in: $SCRIPT_DIR" -ForegroundColor Yellow
    Write-Host "Press Ctrl+C to stop the monitor" -ForegroundColor Yellow
    Write-Host ""

    # Run the monitor script
    & $MONITOR_SCRIPT
}
catch {
    Write-ErrorLog "Fatal error starting monitor: $_"
    Write-Host "Error details have been logged to: $ERROR_LOG" -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}
