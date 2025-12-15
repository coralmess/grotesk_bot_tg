# GroteskBotTg Auto-Update Monitor
# This script monitors for git updates and restarts the bot when updates are found

# Configuration
$SCRIPT_DIR = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$BOT_SCRIPT = Join-Path $SCRIPT_DIR "GroteskBotTg.py"
$LOG_FILE = Join-Path $SCRIPT_DIR "monitor.log"
$LOCK_FILE = Join-Path $SCRIPT_DIR "monitor.lock"
$CHECK_INTERVAL = 600 # 10 minutes in seconds
$GIT_CHECK_INTERVAL = 60 # Check git status every 60 seconds, but only fetch every 10 minutes

# Global variables
$global:BotProcess = $null
$global:LastFetchTime = [DateTime]::MinValue
$global:PythonCommand = $null

# Function to write log with timestamp
function Write-Log {
    param([string]$Message, [string]$Level = "INFO")
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "[$timestamp] [$Level] $Message"
    Write-Host $logMessage
    Add-Content -Path $LOG_FILE -Value $logMessage
}

# Function to create lock file
function Create-LockFile {
    try {
        $currentPid = $PID
        Set-Content -Path $LOCK_FILE -Value $currentPid -Force
        Write-Log "Created lock file with PID: $currentPid"
        return $true
    }
    catch {
        Write-Log "Failed to create lock file: $_" "ERROR"
        return $false
    }
}

# Function to remove lock file
function Remove-LockFile {
    try {
        if (Test-Path $LOCK_FILE) {
            Remove-Item $LOCK_FILE -Force -ErrorAction SilentlyContinue
            Write-Log "Removed lock file"
        }
    }
    catch {
        Write-Log "Failed to remove lock file: $_" "WARNING"
    }
}

# Function to check if monitor is already running
function Check-AlreadyRunning {
    # Check for existing monitor processes (excluding current process)
    $monitorProcesses = Get-Process powershell* -ErrorAction SilentlyContinue | Where-Object {
        try {
            $proc = $_
            if ($proc.Id -eq $PID) { return $false }  # Skip current process
            
            $cmdLine = (Get-CimInstance Win32_Process -Filter "ProcessId = $($proc.Id)" -ErrorAction SilentlyContinue).CommandLine
            $cmdLine -like "*monitor_and_update.ps1*"
        }
        catch {
            $false
        }
    }
    
    if ($monitorProcesses -and $monitorProcesses.Count -gt 0) {
        Write-Log "Another monitor instance is already running (PIDs: $($monitorProcesses.Id -join ', '))" "ERROR"
        Write-Log "Please stop the existing monitor first using stop_all.ps1" "ERROR"
        return $true
    }
    
    # Check lock file
    if (Test-Path $LOCK_FILE) {
        $lockContent = Get-Content $LOCK_FILE -ErrorAction SilentlyContinue
        if ($lockContent) {
            $lockPid = $lockContent[0]
            
            # Skip if lock file is from current process
            if ($lockPid -eq $PID) {
                Write-Log "Lock file is from current process, continuing..." "INFO"
                return $false
            }
            
            $lockProcess = Get-Process -Id $lockPid -ErrorAction SilentlyContinue
            
            if ($lockProcess -and $lockProcess.ProcessName -like "powershell*") {
                Write-Log "Monitor lock file exists for PID $lockPid (still running)" "ERROR"
                Write-Log "Please stop the existing monitor first or delete stale lock file: $LOCK_FILE" "ERROR"
                return $true
            }
            else {
                # Stale lock file, remove it
                Write-Log "Removing stale lock file from PID $lockPid..." "WARNING"
                Remove-LockFile
            }
        }
    }
    
    return $false
}

# Function to send Telegram notification
function Send-TelegramNotification {
    param([string]$Message)
    
    try {
        # Read config to get tokens
        $configPath = Join-Path $SCRIPT_DIR "config.py"
        if (-not (Test-Path $configPath)) {
            Write-Log "config.py not found, cannot send Telegram notification" "WARNING"
            return
        }

        # Read .env file for credentials
        $envPath = Join-Path $SCRIPT_DIR ".env"
        if (-not (Test-Path $envPath)) {
            Write-Log ".env file not found, cannot send Telegram notification" "WARNING"
            return
        }

        $envContent = Get-Content $envPath
        
        # Extract token and chat ID (handle potential arrays from Where-Object)
        # Also handle spaces around = and quotes
        $tokenLine = $envContent | Where-Object { $_ -match "^\s*TELEGRAM_BOT_TOKEN\s*=" } | Select-Object -First 1
        $chatIdLine = $envContent | Where-Object { $_ -match "^\s*DANYLO_DEFAULT_CHAT_ID\s*=" } | Select-Object -First 1
        
        if ([string]::IsNullOrEmpty($tokenLine) -or [string]::IsNullOrEmpty($chatIdLine)) {
            Write-Log ".env is missing TELEGRAM_BOT_TOKEN or DANYLO_DEFAULT_CHAT_ID" "WARNING"
            return
        }
        
        # Parse values - handle spaces and quotes
        $botToken = ($tokenLine -replace "^\s*TELEGRAM_BOT_TOKEN\s*=\s*", "" -replace '["'']', "").Trim()
        $chatId = ($chatIdLine -replace "^\s*DANYLO_DEFAULT_CHAT_ID\s*=\s*", "" -replace '["'']', "").Trim()

        if ([string]::IsNullOrEmpty($botToken) -or [string]::IsNullOrEmpty($chatId)) {
            Write-Log ".env values are empty after parsing" "WARNING"
            return
        }

        $url = "https://api.telegram.org/bot$botToken/sendMessage"
        $body = @{
            chat_id = $chatId
            text = "Bot Monitor - $Message"
            parse_mode = "HTML"
        } | ConvertTo-Json -Depth 10
        
        # Ensure UTF-8 encoding
        $utf8Body = [System.Text.Encoding]::UTF8.GetBytes($body)

        Invoke-RestMethod -Uri $url -Method Post -Body $utf8Body -ContentType "application/json; charset=utf-8" | Out-Null
        Write-Log "Telegram notification sent successfully"
    }
    catch {
        Write-Log "Failed to send Telegram notification: $_" "ERROR"
    }
}

# Function to find Python executable
function Find-Python {
    $pythonCommands = @("python", "python3", "py")
    
    foreach ($cmd in $pythonCommands) {
        try {
            $version = & $cmd --version 2>&1
            if ($LASTEXITCODE -eq 0) {
                Write-Log "Found Python: $cmd ($version)"
                return $cmd
            }
        }
        catch {
            continue
        }
    }
    
    Write-Log "Python not found in PATH!" "ERROR"
    Send-TelegramNotification "Error: Python not found. Please install Python and add it to PATH."
    exit 1
}

# Function to check for git updates
function Check-GitUpdates {
    param([bool]$ForceFetch = $false)
    
    try {
        Push-Location $SCRIPT_DIR
        
        # Only fetch from remote every CHECK_INTERVAL seconds to avoid spamming
        $now = Get-Date
        $timeSinceFetch = ($now - $global:LastFetchTime).TotalSeconds
        
        if ($ForceFetch -or $timeSinceFetch -ge $CHECK_INTERVAL) {
            Write-Log "Fetching from remote repository..."
            $fetchOutput = git fetch origin 2>&1 | Out-String
            
            if ($LASTEXITCODE -ne 0) {
                Write-Log "Git fetch failed: $fetchOutput" "WARNING"
                Write-Log "Continuing with local repository state..." "INFO"
                Pop-Location
                return $false
            }
            
            $global:LastFetchTime = $now
        }
        
        # Check if local is behind remote
        $localCommit = git rev-parse HEAD
        $remoteCommit = git rev-parse origin/master 2>$null
        
        if (-not $remoteCommit) {
            $remoteCommit = git rev-parse origin/main 2>$null
        }
        
        if ($localCommit -ne $remoteCommit) {
            Write-Log "Updates available! Local: $($localCommit.Substring(0,7)) Remote: $($remoteCommit.Substring(0,7))"
            Pop-Location
            return $true
        }
        
        Pop-Location
        return $false
    }
    catch {
        Write-Log "Error checking for updates: $_" "ERROR"
        Pop-Location
        return $false
    }
}

# Function to pull updates
function Pull-Updates {
    try {
        Push-Location $SCRIPT_DIR
        Write-Log "Pulling updates from repository..."
        
        # Get current branch name
        $currentBranch = git rev-parse --abbrev-ref HEAD 2>&1
        if ($LASTEXITCODE -ne 0) {
            Write-Log "Failed to get current branch: $currentBranch" "ERROR"
            Pop-Location
            return $false
        }
        
        Write-Log "Current branch: $currentBranch"
        
        # Pull from origin with explicit branch
        $output = git pull origin $currentBranch 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Log "Successfully pulled updates"
            Write-Log "Git output: $output"
            Pop-Location
            return $true
        }
        else {
            Write-Log "Git pull failed: $output" "ERROR"
            Pop-Location
            return $false
        }
    }
    catch {
        Write-Log "Error pulling updates: $_" "ERROR"
        Pop-Location
        return $false
    }
}

# Function to start the bot
function Start-Bot {
    try {
        if ($global:BotProcess -and -not $global:BotProcess.HasExited) {
            Write-Log "Bot is already running (PID: $($global:BotProcess.Id))"
            return $true
        }

        Write-Log "Starting GroteskBotTg..."
        
        # Start the bot process with visible window
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $global:PythonCommand
        $psi.Arguments = "`"$BOT_SCRIPT`""
        $psi.WorkingDirectory = $SCRIPT_DIR
        $psi.UseShellExecute = $true  # Changed to true to show window
        # Remove these when UseShellExecute = true
        # $psi.RedirectStandardOutput = $true
        # $psi.RedirectStandardError = $true
        # $psi.CreateNoWindow = $true
        
        $global:BotProcess = [System.Diagnostics.Process]::Start($psi)
        
        # Wait a bit to see if it crashes immediately
        Start-Sleep -Seconds 3
        
        if ($global:BotProcess.HasExited) {
            $exitCode = $global:BotProcess.ExitCode
            Write-Log "Bot crashed immediately with exit code: $exitCode" "ERROR"
            Send-TelegramNotification "Error: Bot failed to start (exit code: $exitCode)"
            return $false
        }
        
        Write-Log "Bot started successfully (PID: $($global:BotProcess.Id))"
        Send-TelegramNotification "Bot Started - PID: $($global:BotProcess.Id)"
        return $true
    }
    catch {
        Write-Log "Failed to start bot: $_" "ERROR"
        Send-TelegramNotification "Error: Failed to start bot - $_"
        return $false
    }
}

# Function to stop the bot
function Stop-Bot {
    param([string]$Reason = "manual stop")
    
    try {
        if ($null -eq $global:BotProcess -or $global:BotProcess.HasExited) {
            Write-Log "Bot is not running"
            return $true
        }

        Write-Log "Stopping bot (Reason: $Reason)..."
        
        # Try graceful shutdown first
        $global:BotProcess.Kill()
        $global:BotProcess.WaitForExit(10000) # Wait up to 10 seconds
        
        if (-not $global:BotProcess.HasExited) {
            Write-Log "Bot did not stop gracefully, force killing..." "WARNING"
            $global:BotProcess.Kill($true)
            Start-Sleep -Seconds 2
        }
        
        Write-Log "Bot stopped successfully"
        $global:BotProcess = $null
        return $true
    }
    catch {
        Write-Log "Error stopping bot: $_" "ERROR"
        return $false
    }
}

# Function to update and restart bot
function Update-AndRestart {
    Write-Log "========== UPDATE SEQUENCE STARTED =========="
    
    # Stop the bot
    if (-not (Stop-Bot -Reason "updating")) {
        Write-Log "Failed to stop bot, aborting update" "ERROR"
        return $false
    }
    
    # Pull updates
    if (-not (Pull-Updates)) {
        Write-Log "Failed to pull updates, restarting with old version" "ERROR"
        Start-Bot
        return $false
    }
    
    # Install any new dependencies
    Write-Log "Checking for new dependencies..."
    try {
        & $global:PythonCommand -m pip install -r (Join-Path $SCRIPT_DIR "requirements.txt") --quiet
        Write-Log "Dependencies updated"
    }
    catch {
        Write-Log "Warning: Failed to update dependencies: $_" "WARNING"
    }
    
    # Restart the bot
    if (Start-Bot) {
        Write-Log "========== UPDATE COMPLETED SUCCESSFULLY =========="
        Send-TelegramNotification "Bot Updated Successfully - Pulled latest changes and restarted."
        return $true
    }
    else {
        Write-Log "========== UPDATE FAILED =========="
        Send-TelegramNotification "Update Failed - Bot could not be restarted after update."
        return $false
    }
}

# Main monitoring loop
function Start-Monitoring {
    # Check if already running BEFORE any initialization
    if (Check-AlreadyRunning) {
        Write-Host ""
        Write-Host "ERROR: Cannot start - monitor is already running!" -ForegroundColor Red
        Write-Host "Use stop_all.ps1 to stop the existing instance first." -ForegroundColor Yellow
        exit 1
    }
    
    # Create lock file
    if (-not (Create-LockFile)) {
        Write-Log "Failed to create lock file, aborting" "ERROR"
        exit 1
    }
    
    Write-Log "========== GROTESK BOT MONITOR STARTED =========="
    Write-Log "Monitor PID: $PID"
    Write-Log "Script Directory: $SCRIPT_DIR"
    Write-Log "Check Interval: $CHECK_INTERVAL seconds"
    
    # Find Python
    $global:PythonCommand = Find-Python
    
    # Check git is available
    try {
        git --version | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "Git not found"
        }
    }
    catch {
        Write-Log "Git is not installed or not in PATH!" "ERROR"
        Send-TelegramNotification "Error: Git not found. Please install Git and add it to PATH."
        exit 1
    }
    
    # Initial check for updates
    Write-Log "Performing initial update check..."
    if (Check-GitUpdates -ForceFetch $true) {
        Write-Log "Updates found on startup, applying..."
        Pull-Updates | Out-Null
    }
    
    # Start the bot
    if (-not (Start-Bot)) {
        Write-Log "Failed to start bot on initial run" "ERROR"
        exit 1
    }
    
    # Monitoring loop
    $checkCounter = 0
    while ($true) {
        Start-Sleep -Seconds $GIT_CHECK_INTERVAL
        $checkCounter++
        
        # Check if bot is still running
        if ($global:BotProcess -and $global:BotProcess.HasExited) {
            $exitCode = $global:BotProcess.ExitCode
            Write-Log "Bot process has exited unexpectedly (exit code: $exitCode)" "ERROR"
            Send-TelegramNotification "Bot Crashed - Exit code: $exitCode - Restarting..."
            Start-Bot
            continue
        }
        
        # Check for updates (fetch only every CHECK_INTERVAL)
        if (Check-GitUpdates) {
            Write-Log "Updates detected, initiating update sequence..."
            Update-AndRestart
        }
        
        # Log heartbeat every 10 checks
        if ($checkCounter % 10 -eq 0) {
            if ($global:BotProcess -and -not $global:BotProcess.HasExited) {
                Write-Log "Heartbeat: Bot running (PID: $($global:BotProcess.Id)), checked $checkCounter times"
            }
        }
    }
}

# Handle Ctrl+C gracefully
try {
    Start-Monitoring
}
finally {
    Write-Log "Monitor shutting down..."
    Stop-Bot -Reason "monitor shutdown"
    Remove-LockFile
    Write-Log "========== MONITOR STOPPED =========="
}
