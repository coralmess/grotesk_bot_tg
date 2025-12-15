# Stop All GroteskBotTg Processes
# This script stops ALL bot-related processes and the monitor

# Set console encoding for proper emoji display
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$SCRIPT_DIR = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$TASK_NAME = "GroteskBotTg-AutoStart"
$LOCK_FILE = Join-Path $SCRIPT_DIR "monitor.lock"

Write-Host ""
Write-Host "========================================" -ForegroundColor Red
Write-Host "  STOP All GroteskBotTg Processes" -ForegroundColor Red
Write-Host "========================================" -ForegroundColor Red
Write-Host ""

$stoppedSomething = $false

# 1. Stop the scheduled task if it's running
Write-Host "Checking scheduled task..." -ForegroundColor Yellow
$task = Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue
if ($existingTask -and $task.State -eq "Running") {
    try {
        Stop-ScheduledTask -TaskName $TASK_NAME -ErrorAction Stop
        Write-Host "  [OK] Stopped scheduled task '$TASK_NAME'" -ForegroundColor Green
        $stoppedSomething = $true
        Start-Sleep -Seconds 2
    }
    catch {
        Write-Host "  [!] Failed to stop scheduled task: $_" -ForegroundColor Yellow
    }
}
elseif ($task) {
    Write-Host "  [i] Scheduled task exists but is not running" -ForegroundColor Gray
}
else {
    Write-Host "  [i] No scheduled task found" -ForegroundColor Gray
}

# 2. Find and kill all Python processes running GroteskBotTg.py
Write-Host "`nStopping GroteskBotTg processes..." -ForegroundColor Yellow
$botProcesses = Get-Process python* -ErrorAction SilentlyContinue | Where-Object {
    try {
        $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)").CommandLine
        $cmdLine -like "*GroteskBotTg.py*"
    }
    catch {
        $false
    }
}

if ($botProcesses) {
    foreach ($proc in $botProcesses) {
        try {
            Write-Host "  Stopping bot process (PID: $($proc.Id))..." -ForegroundColor Yellow
            Stop-Process -Id $proc.Id -Force -ErrorAction Stop
            Write-Host "  [OK] Stopped bot process (PID: $($proc.Id))" -ForegroundColor Green
            $stoppedSomething = $true
        }
        catch {
            Write-Host "  [!] Failed to stop PID $($proc.Id): $_" -ForegroundColor Yellow
        }
    }
}
else {
    Write-Host "  [i] No GroteskBotTg processes found" -ForegroundColor Gray
}

# 3. Find and kill monitor PowerShell processes
Write-Host "`nStopping monitor processes..." -ForegroundColor Yellow
$monitorProcesses = Get-Process powershell* -ErrorAction SilentlyContinue | Where-Object {
    try {
        $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)").CommandLine
        $cmdLine -like "*monitor_and_update.ps1*" -or $cmdLine -like "*start_monitor.ps1*"
    }
    catch {
        $false
    }
}

if ($monitorProcesses) {
    foreach ($proc in $monitorProcesses) {
        try {
            Write-Host "  Stopping monitor process (PID: $($proc.Id))..." -ForegroundColor Yellow
            Stop-Process -Id $proc.Id -Force -ErrorAction Stop
            Write-Host "  [OK] Stopped monitor process (PID: $($proc.Id))" -ForegroundColor Green
            $stoppedSomething = $true
        }
        catch {
            Write-Host "  [!] Failed to stop PID $($proc.Id): $_" -ForegroundColor Yellow
        }
    }
}
else {
    Write-Host "  [i] No monitor processes found" -ForegroundColor Gray
}

# 4. Look for any other Python processes in this directory (extra safety)
Write-Host "`nChecking for other Python processes in this directory..." -ForegroundColor Yellow
$otherPythonProcesses = Get-Process python* -ErrorAction SilentlyContinue | Where-Object {
    try {
        $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)").CommandLine
        $cmdLine -like "*$SCRIPT_DIR*" -and $cmdLine -notlike "*GroteskBotTg.py*"
    }
    catch {
        $false
    }
}

if ($otherPythonProcesses) {
    Write-Host "  Found $($otherPythonProcesses.Count) other Python process(es):" -ForegroundColor Yellow
    foreach ($proc in $otherPythonProcesses) {
        try {
            $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)").CommandLine
            Write-Host "    PID $($proc.Id): $cmdLine" -ForegroundColor Gray
            
            $response = Read-Host "    Stop this process? (y/n)"
            if ($response -eq 'y' -or $response -eq 'Y') {
                Stop-Process -Id $proc.Id -Force -ErrorAction Stop
                Write-Host "    [OK] Stopped PID $($proc.Id)" -ForegroundColor Green
                $stoppedSomething = $true
            }
        }
        catch {
            Write-Host "    [!] Failed to stop PID $($proc.Id): $_" -ForegroundColor Yellow
        }
    }
}
else {
    Write-Host "  [i] No other Python processes found" -ForegroundColor Gray
}

# Clean up lock file
Write-Host "`nCleaning up lock file..." -ForegroundColor Yellow
if (Test-Path $LOCK_FILE) {
    try {
        Remove-Item $LOCK_FILE -Force -ErrorAction Stop
        Write-Host "  [OK] Removed monitor lock file" -ForegroundColor Green
        $stoppedSomething = $true
    }
    catch {
        Write-Host "  [!] Failed to remove lock file: $_" -ForegroundColor Yellow
    }
}
else {
    Write-Host "  [i] No lock file found" -ForegroundColor Gray
}

# Summary
Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
if ($stoppedSomething) {
    Write-Host "  [OK] All processes stopped" -ForegroundColor Green
    Write-Host ""
    Write-Host "To restart:" -ForegroundColor Yellow
    Write-Host "  - Manual start: Double-click START_BOT.bat" -ForegroundColor White
    Write-Host "  - Auto-start task: Start-ScheduledTask -TaskName '$TASK_NAME'" -ForegroundColor White
}
else {
    Write-Host "  [i] No running processes found" -ForegroundColor Gray
    Write-Host "  Bot was already stopped" -ForegroundColor White
}
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
