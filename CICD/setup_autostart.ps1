# Setup script to configure Windows Task Scheduler for auto-start on boot
# Run this script as Administrator to set up automatic startup

$SCRIPT_DIR = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$STARTUP_SCRIPT = Join-Path $SCRIPT_DIR "CICD\start_monitor.ps1"
$TASK_NAME = "GroteskBotTg-AutoStart"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "GroteskBotTg Auto-Start Setup" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check if running as administrator
$isAdmin = ([Security.Principal.WindowsPrincipal] [Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)

if (-not $isAdmin) {
    Write-Host "ERROR: This script must be run as Administrator!" -ForegroundColor Red
    Write-Host "Right-click on this script and select 'Run as Administrator'" -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 1
}

# Check if task already exists
$existingTask = Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue

if ($existingTask) {
    Write-Host "Existing auto-start task found." -ForegroundColor Yellow
    $response = Read-Host "Do you want to remove it and create a new one? (y/n)"
    
    if ($response -eq 'y' -or $response -eq 'Y') {
        Unregister-ScheduledTask -TaskName $TASK_NAME -Confirm:$false
        Write-Host "Existing task removed." -ForegroundColor Green
    }
    else {
        Write-Host "Setup cancelled." -ForegroundColor Yellow
        Read-Host "Press Enter to exit"
        exit 0
    }
}

Write-Host ""
Write-Host "Creating scheduled task for auto-start on boot..." -ForegroundColor Cyan

# Create the action (what to run)
$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-ExecutionPolicy Bypass -WindowStyle Hidden -File `"$STARTUP_SCRIPT`"" `
    -WorkingDirectory $SCRIPT_DIR

# Create the trigger (when to run)
$trigger = New-ScheduledTaskTrigger -AtStartup

# Create the principal (run with highest privileges)
$principal = New-ScheduledTaskPrincipal `
    -UserId "$env:USERDOMAIN\$env:USERNAME" `
    -LogonType Interactive `
    -RunLevel Highest

# Create task settings
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -ExecutionTimeLimit (New-TimeSpan -Days 365)

# Register the task
try {
    Register-ScheduledTask `
        -TaskName $TASK_NAME `
        -Action $action `
        -Trigger $trigger `
        -Principal $principal `
        -Settings $settings `
        -Description "Automatically start and monitor GroteskBotTg with auto-update functionality" `
        -ErrorAction Stop | Out-Null
    
    Write-Host ""
    Write-Host "SUCCESS! Auto-start task has been created." -ForegroundColor Green
    Write-Host ""
    Write-Host "Task Details:" -ForegroundColor Cyan
    Write-Host "  Name: $TASK_NAME" -ForegroundColor White
    Write-Host "  Trigger: At system startup" -ForegroundColor White
    Write-Host "  Script: $STARTUP_SCRIPT" -ForegroundColor White
    Write-Host ""
    Write-Host "The bot will now automatically start when Windows boots." -ForegroundColor Green
    Write-Host ""
    Write-Host "To manage the task:" -ForegroundColor Yellow
    Write-Host "  1. Open Task Scheduler (taskschd.msc)" -ForegroundColor White
    Write-Host "  2. Find '$TASK_NAME' in the Task Scheduler Library" -ForegroundColor White
    Write-Host ""
    
    $runNow = Read-Host "Do you want to start the task now? (y/n)"
    
    if ($runNow -eq 'y' -or $runNow -eq 'Y') {
        Start-ScheduledTask -TaskName $TASK_NAME
        Write-Host ""
        Write-Host "Task started! The bot monitor is now running in the background." -ForegroundColor Green
        Write-Host "Check monitor.log for details: $(Join-Path $SCRIPT_DIR 'monitor.log')" -ForegroundColor Yellow
    }
}
catch {
    Write-Host ""
    Write-Host "ERROR: Failed to create scheduled task" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Read-Host "Press Enter to exit"
