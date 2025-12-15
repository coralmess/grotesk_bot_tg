# Status Check Script for GroteskBotTg Monitor
# Shows current status of the monitoring system

# Set console encoding
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8
$OutputEncoding = [System.Text.Encoding]::UTF8

$SCRIPT_DIR = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$TASK_NAME = "GroteskBotTg-AutoStart"
$LOG_FILE = Join-Path $SCRIPT_DIR "monitor.log"

function Get-ColoredStatus {
    param([bool]$IsGood, [string]$GoodText, [string]$BadText)
    if ($IsGood) {
        Write-Host "  [OK] $GoodText" -ForegroundColor Green
    } else {
        Write-Host "  [X] $BadText" -ForegroundColor Red
    }
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  GroteskBotTg Monitor Status" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Check Python
Write-Host "Python:" -ForegroundColor Yellow
try {
    $pythonVersion = python --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] $pythonVersion" -ForegroundColor Green
    } else {
        Write-Host "  [X] Python not found in PATH" -ForegroundColor Red
    }
} catch {
    Write-Host "  [X] Python not found" -ForegroundColor Red
}

# Check Git
Write-Host "`nGit:" -ForegroundColor Yellow
try {
    $gitVersion = git --version 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "  [OK] $gitVersion" -ForegroundColor Green
    } else {
        Write-Host "  [X] Git not found in PATH" -ForegroundColor Red
    }
} catch {
    Write-Host "  [X] Git not found" -ForegroundColor Red
}

# Check scheduled task
Write-Host "`nScheduled Task:" -ForegroundColor Yellow
$task = Get-ScheduledTask -TaskName $TASK_NAME -ErrorAction SilentlyContinue
if ($task) {
    Write-Host "  [OK] Task exists" -ForegroundColor Green
    Write-Host "     State: $($task.State)" -ForegroundColor White
    
    $taskInfo = Get-ScheduledTaskInfo -TaskName $TASK_NAME
    if ($taskInfo.LastRunTime) {
        Write-Host "     Last Run: $($taskInfo.LastRunTime)" -ForegroundColor White
    }
    if ($taskInfo.LastTaskResult -eq 0) {
        Write-Host "     Last Result: Success" -ForegroundColor Green
    } elseif ($taskInfo.LastTaskResult) {
        Write-Host "     Last Result: Error ($($taskInfo.LastTaskResult))" -ForegroundColor Red
    }
} else {
    Write-Host "  [X] Auto-start task not configured" -ForegroundColor Red
    Write-Host "     Run setup_autostart.ps1 as Administrator to enable" -ForegroundColor Yellow
}

# Check if bot process is running
Write-Host "`nBot Process:" -ForegroundColor Yellow
$botProcesses = Get-Process python* -ErrorAction SilentlyContinue | Where-Object {
    $_.CommandLine -like "*GroteskBotTg.py*"
}
if ($botProcesses) {
    Write-Host "  [OK] Bot is running" -ForegroundColor Green
    foreach ($proc in $botProcesses) {
        Write-Host "     PID: $($proc.Id), Started: $($proc.StartTime)" -ForegroundColor White
    }
} else {
    Write-Host "  [X] Bot process not found" -ForegroundColor Red
}

# Check log file
Write-Host "`nMonitor Log:" -ForegroundColor Yellow
if (Test-Path $LOG_FILE) {
    $logInfo = Get-Item $LOG_FILE
    Write-Host "  [OK] Log file exists" -ForegroundColor Green
    Write-Host "     Size: $([math]::Round($logInfo.Length / 1KB, 2)) KB" -ForegroundColor White
    Write-Host "     Last Modified: $($logInfo.LastWriteTime)" -ForegroundColor White
    
    # Show last few lines
    Write-Host "`n  Last 5 log entries:" -ForegroundColor Cyan
    Get-Content $LOG_FILE -Tail 5 | ForEach-Object {
        Write-Host "    $_" -ForegroundColor Gray
    }
} else {
    Write-Host "  ⚠️  Log file not found (monitor hasn't run yet)" -ForegroundColor Yellow
}

# Check git status
Write-Host "`nGit Repository:" -ForegroundColor Yellow
try {
    Push-Location $SCRIPT_DIR
    
    $branch = git rev-parse --abbrev-ref HEAD 2>$null
    if ($branch) {
        Write-Host "  [OK] Git repository initialized" -ForegroundColor Green
        Write-Host "     Branch: $branch" -ForegroundColor White
        
        $status = git status --porcelain
        if ($status) {
            Write-Host "     Uncommitted changes: Yes" -ForegroundColor Yellow
        } else {
            Write-Host "     Uncommitted changes: No" -ForegroundColor Green
        }
        
        # Check if remote is configured
        $remote = git remote -v 2>$null | Select-Object -First 1
        if ($remote) {
            Write-Host "     Remote: Configured" -ForegroundColor Green
        } else {
            Write-Host "     Remote: Not configured" -ForegroundColor Yellow
        }
    } else {
        Write-Host "  [X] Not a git repository" -ForegroundColor Red
    }
    
    Pop-Location
} catch {
    Write-Host "  [X] Error checking git status" -ForegroundColor Red
    Pop-Location
}

# Check .env file
Write-Host "`nConfiguration:" -ForegroundColor Yellow
$envPath = Join-Path $SCRIPT_DIR ".env"
if (Test-Path $envPath) {
    Write-Host "  [OK] .env file exists" -ForegroundColor Green
    
    $envContent = Get-Content $envPath
    $hasToken = ($envContent | Where-Object { $_ -match "^TELEGRAM_BOT_TOKEN=" }) -ne $null
    $hasChatId = ($envContent | Where-Object { $_ -match "^DANYLO_DEFAULT_CHAT_ID=" }) -ne $null
    
    Get-ColoredStatus $hasToken "TELEGRAM_BOT_TOKEN configured" "TELEGRAM_BOT_TOKEN missing"
    Get-ColoredStatus $hasChatId "DANYLO_DEFAULT_CHAT_ID configured" "DANYLO_DEFAULT_CHAT_ID missing"
} else {
    Write-Host "  [X] .env file not found" -ForegroundColor Red
}

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Suggestions
if (-not $task) {
    Write-Host "💡 Tip: Run 'setup_autostart.ps1' as Admin to enable auto-start" -ForegroundColor Yellow
}
if (-not $botProcesses -and $task) {
    Write-Host "💡 Tip: Run 'Start-ScheduledTask -TaskName `"$TASK_NAME`"' to start the bot" -ForegroundColor Yellow
}

Write-Host ""
