# Duplicate Instance Protection

## Overview

The CI/CD scripts now include comprehensive protection against running duplicate bot instances. This prevents resource conflicts, database locks, and message duplication.

## Protection Mechanisms

### 1. Lock File (`monitor.lock`)

- **Location**: `monitor.lock` in the root directory
- **Content**: Process ID (PID) of the running monitor
- **Purpose**: Persistent indicator that a monitor is running

When the monitor starts:
- Creates `monitor.lock` with its PID
- Checks if lock file exists and validates the PID
- Removes stale lock files (process no longer running)

When the monitor stops:
- Automatically removes the lock file
- Cleanup happens even on Ctrl+C or errors (via `finally` block)

### 2. Process Detection

Both `start_monitor.ps1` and `monitor_and_update.ps1` check for:

- **Monitor processes**: PowerShell processes running `monitor_and_update.ps1`
- **Bot processes**: Python processes running `GroteskBotTg.py`

Detection uses:
```powershell
Get-Process powershell* | Where-Object {
    (Get-CimInstance Win32_Process -Filter "ProcessId = $($_.Id)").CommandLine -like "*monitor_and_update.ps1*"
}
```

### 3. Validation Flow

#### When Starting Monitor (`start_monitor.ps1`):

1. ✅ Check for running monitor processes (excluding self)
2. ✅ Check lock file existence
3. ✅ Validate PID in lock file is still running
4. ✅ Remove stale locks automatically
5. ❌ Exit with error if active instance found

#### During Monitor Execution (`monitor_and_update.ps1`):

1. ✅ Check for duplicates BEFORE initialization
2. ✅ Create lock file with current PID
3. ✅ Monitor continues normally
4. ✅ Lock file removed on shutdown (graceful or forced)

## User Experience

### Attempting to Start Duplicate

```
Monitor is already running (PIDs: 12345)
If you want to restart it, run STOP_ALL.bat first

Press Enter to exit
```

### From Monitor Script

```
[2025-12-15 10:30:15] [ERROR] Another monitor instance is already running (PIDs: 12345)
[2025-12-15 10:30:15] [ERROR] Please stop the existing monitor first using stop_all.ps1

ERROR: Cannot start - monitor is already running!
Use stop_all.ps1 to stop the existing instance first.
```

## Stale Lock Handling

If a lock file exists but the process is no longer running:

- ✅ Automatically detected as "stale"
- ✅ Logged with warning
- ✅ Lock file removed automatically
- ✅ New instance allowed to start

Example:
```
[2025-12-15 10:30:15] [WARNING] Removing stale lock file from PID 12345...
[2025-12-15 10:30:15] [INFO] Created lock file with PID: 67890
```

## Status Checking

Use `check_status.ps1` or `CHECK_STATUS.bat` to see:

- Monitor process status and PID
- Bot process status and PID
- Lock file status (valid/stale/none)
- Recommendations for cleanup

Example output:
```
Monitor Process:
  [OK] Monitor is running
     PID: 12345, Started: 12/15/2025 10:00:00 AM

Monitor Lock File:
  [OK] Lock file exists (PID: 12345, process running)
```

## Cleanup

### Manual Cleanup

```powershell
# Stop all processes and clean lock file
.\STOP_ALL.bat

# Or use PowerShell directly
.\CICD\stop_all.ps1
```

### What `stop_all.ps1` Does

1. ✅ Stops scheduled tasks
2. ✅ Kills all bot processes
3. ✅ Kills all monitor processes
4. ✅ Removes lock file
5. ✅ Reports status of each action

## Edge Cases Handled

| Scenario | Behavior |
|----------|----------|
| Lock file exists, process running | ❌ Prevent startup, show error |
| Lock file exists, process dead | ✅ Remove lock, allow startup |
| No lock file, process running | ❌ Prevent startup (process check) |
| No lock file, no process | ✅ Allow startup normally |
| Crash during startup | ✅ Lock removed via `finally` block |
| Ctrl+C during execution | ✅ Lock removed via `finally` block |
| Multiple start attempts | ❌ All blocked except first |

## Technical Details

### Lock File Format
```
12345
```
(Single line containing the monitor's PID)

### Files Modified

- `CICD/start_monitor.ps1` - Added duplicate detection before starting
- `CICD/monitor_and_update.ps1` - Added lock file management and duplicate prevention
- `CICD/stop_all.ps1` - Added lock file cleanup
- `CICD/check_status.ps1` - Added lock file status display

### Key Functions

**monitor_and_update.ps1**:
- `Check-AlreadyRunning()` - Validates no duplicate instance
- `Create-LockFile()` - Creates lock with current PID
- `Remove-LockFile()` - Cleans up lock file
- `finally` block - Ensures cleanup on exit

**start_monitor.ps1**:
- `Check-MonitorRunning()` - Pre-flight duplicate check

## Benefits

✅ **Prevents conflicts**: No duplicate bots sending duplicate messages
✅ **Database safety**: Prevents SQLite locking issues
✅ **Resource efficiency**: No wasted CPU/memory on duplicates
✅ **Clear errors**: Users know exactly what's wrong
✅ **Auto-recovery**: Stale locks cleaned automatically
✅ **Robust cleanup**: Lock removed even on crashes/Ctrl+C

## Testing

To verify protection works:

1. Start monitor: `.\START_BOT.bat`
2. Try starting again: `.\START_BOT.bat` (should fail with clear message)
3. Check status: `.\CHECK_STATUS.bat` (should show running instance)
4. Stop all: `.\STOP_ALL.bat` (should clean everything)
5. Start again: `.\START_BOT.bat` (should work now)
