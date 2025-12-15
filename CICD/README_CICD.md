# GroteskBotTg CI/CD System - README

## Overview
This CI/CD system automatically monitors for updates and manages the GroteskBotTg bot lifecycle on Windows 10.

## Features
- ✅ Auto-starts bot on Windows startup
- ✅ Checks for git updates every 10 minutes
- ✅ Automatically pulls updates and restarts bot
- ✅ Sends Telegram notifications on updates and errors
- ✅ Monitors bot health and restarts if crashed
- ✅ Detailed logging of all operations

## Files Included
1. **monitor_and_update.ps1** - Main monitoring script that handles updates and bot lifecycle
2. **start_monitor.ps1** - Startup wrapper with error handling
3. **START_BOT.bat** - Double-click to manually start the monitor
4. **setup_autostart.ps1** - Configure Windows Task Scheduler for auto-start on boot
5. **README_CICD.md** - This file

## Quick Start

### Option 1: Manual Start (Testing)
Simply double-click `START_BOT.bat` to start the monitor manually. This is useful for testing.

### Option 2: Auto-Start on Windows Boot (Recommended)
1. **Right-click** on `setup_autostart.ps1`
2. Select **"Run with PowerShell as Administrator"**
3. Follow the prompts to create the scheduled task
4. The bot will now start automatically when Windows boots

## How It Works

### Update Checking
- Every 60 seconds: Checks if local code is behind remote repository
- Every 10 minutes: Fetches from remote to get latest commit info
- This minimizes git server requests while still detecting updates quickly

### Update Process
When updates are detected:
1. Bot is gracefully stopped
2. Latest code is pulled from git
3. Python dependencies are updated from `requirements.txt`
4. Bot is restarted with new code
5. Telegram notification is sent to confirm successful update

### Bot Monitoring
- Continuously monitors if bot process is running
- Automatically restarts bot if it crashes
- Sends Telegram notification on crashes

### Logging
All activities are logged to `monitor.log` in the same directory:
- Bot starts/stops
- Update checks and pulls
- Errors and warnings
- Heartbeat messages every 10 checks

## Telegram Notifications

The monitor sends notifications for:
- ✅ Bot started successfully
- 🔄 Bot updated successfully
- ❌ Errors (Python not found, git not found, etc.)
- ⚠️ Bot crashed and restarting

Notifications are sent using `TELEGRAM_BOT_TOKEN` and `DANYLO_DEFAULT_CHAT_ID` from your `config.py`/.env file.

## Requirements

### Software
- **Windows 10** (or newer)
- **Python 3.x** installed and in PATH
- **Git** installed and in PATH
- **PowerShell 5.1+** (included with Windows 10)

### Repository Setup
- Git repository must be initialized
- Remote repository (e.g., GitHub) must be configured
- You should be on the `master` or `main` branch

### Configuration Files
- `.env` file with `TELEGRAM_BOT_TOKEN` and `DANYLO_DEFAULT_CHAT_ID`
- `requirements.txt` for Python dependencies
- `config.py` properly configured

## Managing the Auto-Start Task

### View Task Status
1. Press `Win + R`, type `taskschd.msc`, press Enter
2. Find `GroteskBotTg-AutoStart` in the Task Scheduler Library
3. Right-click to Run, Stop, Disable, or Delete

### Stop the Monitor
**From Task Scheduler:**
- Right-click task → End

**From Command Line:**
```powershell
Stop-ScheduledTask -TaskName "GroteskBotTg-AutoStart"
```

### Remove Auto-Start
**From Task Scheduler:**
- Right-click task → Delete

**From Command Line:**
```powershell
Unregister-ScheduledTask -TaskName "GroteskBotTg-AutoStart" -Confirm:$false
```

## Troubleshooting

### Bot doesn't start
1. Check `monitor.log` for errors
2. Check `startup_error.log` if present
3. Verify Python is installed: `python --version`
4. Verify Git is installed: `git --version`
5. Make sure `.env` file exists with proper credentials

### Updates not detected
1. Check git remote is configured: `git remote -v`
2. Verify internet connection
3. Check `monitor.log` for git errors
4. Try manual git fetch: `git fetch origin`

### No Telegram notifications
1. Verify `.env` file contains `TELEGRAM_BOT_TOKEN` and `DANYLO_DEFAULT_CHAT_ID`
2. Test bot token manually
3. Check `monitor.log` for notification errors

### Task doesn't run at startup
1. Verify you ran `setup_autostart.ps1` as Administrator
2. Check Task Scheduler for the task
3. Check task history in Task Scheduler
4. Ensure user account has permissions

## Manual Operations

### Start Monitor Manually
```powershell
cd "d:\Chrome\911\LystTgFirefox"
.\start_monitor.ps1
```

### Check for Updates Without Restarting
```powershell
cd "d:\Chrome\911\LystTgFirefox"
git fetch origin
git status
```

### Pull Updates Manually
```powershell
cd "d:\Chrome\911\LystTgFirefox"
git pull origin master
pip install -r requirements.txt
```

### View Logs
```powershell
# View monitor log
Get-Content "d:\Chrome\911\LystTgFirefox\monitor.log" -Tail 50

# Live monitoring
Get-Content "d:\Chrome\911\LystTgFirefox\monitor.log" -Wait
```

## Advanced Configuration

### Change Check Interval
Edit `monitor_and_update.ps1`:
```powershell
$CHECK_INTERVAL = 600  # Change to desired seconds (default: 600 = 10 minutes)
```

### Disable Telegram Notifications
Comment out `Send-TelegramNotification` calls in `monitor_and_update.ps1`

### Custom Branch
By default, the monitor tracks `origin/master`. To change:

Edit `monitor_and_update.ps1`, find:
```powershell
$remoteCommit = git rev-parse origin/master 2>$null
```
Change `origin/master` to your branch (e.g., `origin/develop`)

## Security Notes

1. **Credentials**: Never commit your `.env` file to git
2. **Execution Policy**: Scripts use `-ExecutionPolicy Bypass` for convenience. Review scripts before running.
3. **Admin Rights**: Setup requires admin rights for Task Scheduler, but monitor runs as current user

## Support

If you encounter issues:
1. Check all log files (`monitor.log`, `startup_error.log`)
2. Verify all requirements are met
3. Test components individually (Python, Git, Telegram API)
4. Review error messages in logs

## License
Same as GroteskBotTg project.
