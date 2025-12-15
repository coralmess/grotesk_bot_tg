# Startup Script for GroteskBotTg Monitor
# This script ensures the monitor runs with proper permissions and error handling

$SCRIPT_DIR = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$MONITOR_SCRIPT = Join-Path $SCRIPT_DIR "CICD\monitor_and_update.ps1"
$ERROR_LOG = Join-Path $SCRIPT_DIR "startup_error.log"
$LOCK_FILE = Join-Path $SCRIPT_DIR "monitor.lock"

function Write-ErrorLog {
    param([string]$Message)
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Add-Content -Path $ERROR_LOG -Value "[$timestamp] $Message"
    Write-Host $Message -ForegroundColor Red
}

function Check-MonitorRunning {
    # Check for existing monitor processes
    $monitorProcesses = Get-Process powershell* -ErrorAction SilentlyContinue | Where-Object {
        try {
            $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)" -ErrorAction SilentlyContinue).CommandLine
            $cmdLine -like "*monitor_and_update.ps1*"
        }
        catch {
            $false
        }
    }
    
    if ($monitorProcesses -and $monitorProcesses.Count -gt 0) {
        Write-Host "Monitor is already running (PIDs: $($monitorProcesses.Id -join ', '))" -ForegroundColor Yellow
        Write-Host "If you want to restart it, run STOP_ALL.bat first" -ForegroundColor Yellow
        return $true
    }
    
    # Check lock file
    if (Test-Path $LOCK_FILE) {
        $lockContent = Get-Content $LOCK_FILE -ErrorAction SilentlyContinue
        if ($lockContent) {
            $lockPid = $lockContent[0]
            $lockProcess = Get-Process -Id $lockPid -ErrorAction SilentlyContinue
            
            if ($lockProcess -and $lockProcess.ProcessName -like "powershell*") {
                Write-Host "Monitor lock file exists for PID $lockPid (still running)" -ForegroundColor Yellow
                Write-Host "If this is incorrect, delete the lock file: $LOCK_FILE" -ForegroundColor Yellow
                return $true
            }
            else {
                # Stale lock file, remove it
                Write-Host "Removing stale lock file..." -ForegroundColor Gray
                Remove-Item $LOCK_FILE -Force -ErrorAction SilentlyContinue
            }
        }
    }
    
    return $false
}

try {
    # Check if monitor is already running
    if (Check-MonitorRunning) {
        Write-Host ""
        Read-Host "Press Enter to exit"
        exit 0
    }
    
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
