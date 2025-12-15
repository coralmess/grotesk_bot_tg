# Uninstall script for GroteskBotTg Auto-Start
# Removes the scheduled task and stops the monitor

$TASK_NAME = "GroteskBotTg-AutoStart"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host "GroteskBotTg Auto-Start Removal" -ForegroundColor Cyan
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

# Check if task exists
$existingTask = Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue

if (-not $existingTask) {
    Write-Host "No auto-start task found." -ForegroundColor Yellow
    Write-Host "Task '$TASK_NAME' does not exist." -ForegroundColor White
    Read-Host "Press Enter to exit"
    exit 0
}

Write-Host "Found auto-start task: $TASK_NAME" -ForegroundColor Cyan
Write-Host ""

$confirm = Read-Host "Are you sure you want to remove the auto-start task? (y/n)"

if ($confirm -ne 'y' -and $confirm -ne 'Y') {
    Write-Host "Removal cancelled." -ForegroundColor Yellow
    Read-Host "Press Enter to exit"
    exit 0
}

try {
    # Stop the task if running
    Write-Host "Stopping task if running..." -ForegroundColor Yellow
    Stop-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue
    
    # Remove the task
    Write-Host "Removing scheduled task..." -ForegroundColor Yellow
    Unregister-ScheduledTask -TaskName $TASK_NAME -Confirm:$false
    
    Write-Host ""
    Write-Host "SUCCESS! Auto-start task has been removed." -ForegroundColor Green
    Write-Host ""
    Write-Host "The bot will no longer start automatically on Windows boot." -ForegroundColor White
    Write-Host "You can still start it manually using START_BOT.bat" -ForegroundColor White
}
catch {
    Write-Host ""
    Write-Host "ERROR: Failed to remove scheduled task" -ForegroundColor Red
    Write-Host $_.Exception.Message -ForegroundColor Red
    Read-Host "Press Enter to exit"
    exit 1
}

Write-Host ""
Read-Host "Press Enter to exit"
