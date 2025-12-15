# GroteskBotTg CI/CD - Quick Start Guide

## 🚀 First Time Setup

### Step 1: Install Requirements
Make sure you have:
- ✅ Python 3.x (in PATH)
- ✅ Git (in PATH)
- ✅ Your `.env` file configured

Test with:
```powershell
python --version
git --version
```

### Step 2: Choose Your Method

#### Method A: Auto-Start on Boot (Recommended)
1. **Right-click** `setup_autostart.ps1`
2. Click **"Run with PowerShell as Administrator"**
3. Follow prompts and select "Yes" to start now
4. Done! Bot will auto-start on every Windows boot

#### Method B: Manual Start (Testing)
1. **Double-click** `START_BOT.bat`
2. Bot starts running
3. Keep window open (close to stop)

## 📊 Daily Usage

### Check if Bot is Running
- Look for `monitor.log` file growing in size
- Check Task Scheduler: `Win+R` → `taskschd.msc` → Find "GroteskBotTg-AutoStart"
- You'll get Telegram notifications when it starts/updates

### View Logs
**Windows Explorer:**
- Open `monitor.log` with Notepad

**PowerShell:**
```powershell
# View last 50 lines
Get-Content monitor.log -Tail 50

# Live view (updates as log grows)
Get-Content monitor.log -Wait
```

### Stop the Bot
**Quick Method:**
- **Double-click:** `STOP_ALL.bat`

**If using Auto-Start:**
```powershell
Stop-ScheduledTask -TaskName "GroteskBotTg-AutoStart"
```

**If using Manual Start:**
- Close the window or press `Ctrl+C`

**Stop Everything (Nuclear Option):**
```powershell
.\stop_all.ps1
# Stops: scheduled task, bot processes, monitor processes
```

### Restart the Bot
**If using Auto-Start:**
```powershell
Restart-Computer  # Full restart, or:
Start-ScheduledTask -TaskName "GroteskBotTg-AutoStart"
```

**If using Manual Start:**
- Close window and double-click `START_BOT.bat` again

## 🔄 Updates

**Automatic** (Default):
- Monitor checks for updates every 10 minutes
- Auto-pulls and restarts bot
- Sends you Telegram notification
- No action needed! ✨

**Manual**:
```powershell
cd "d:\Chrome\911\LystTgFirefox"
git pull origin master
# Then restart bot
```

## 🛠️ Management

### Disable Auto-Start
```powershell
# Option 1: Right-click remove_autostart.ps1 → Run as Admin

# Option 2: PowerShell command
Unregister-ScheduledTask -TaskName "GroteskBotTg-AutoStart" -Confirm:$false
```

### Re-enable Auto-Start
Right-click `setup_autostart.ps1` → Run as Administrator

## 🆘 Troubleshooting

### Bot Not Starting
```powershell
# Check monitor log
Get-Content monitor.log -Tail 20

# Check if Python works
python --version

# Check if Git works
git status

# Try manual start to see errors
.\start_monitor.ps1
```

**Common issues:**
- Git fetch failed → See `TROUBLESHOOTING.md`
- Telegram token not found → Check `.env` file format
- Python/Git not found → Add to system PATH

### No Telegram Notifications
1. Check `.env` file exists and has correct format:
   ```
   TELEGRAM_BOT_TOKEN=your_token_here
   DANYLO_DEFAULT_CHAT_ID=your_chat_id_here
   ```
2. No quotes, no spaces around `=`
3. Check `TROUBLESHOOTING.md` for detailed steps

### Bot Keeps Restarting
- Check `monitor.log` for error messages
- Check `GroteskBotTg.py` logs
- May indicate code error - check recent commits

### Updates Not Working
```powershell
# Check git remote
git remote -v

# Manual fetch
git fetch origin

# Check current status
git status
```

## 📱 Telegram Notifications You'll See

- ✅ **"Bot Started"** - Bot successfully started
- 🔄 **"Bot Updated Successfully"** - Auto-update completed
- ⚠️ **"Bot Crashed"** - Bot died, being restarted
- ❌ **"Error: ..."** - Something went wrong

## 📁 Files Reference

| File | Purpose |
|------|---------|
| `START_BOT.bat` | Double-click to start (manual) |
| `STOP_ALL.bat` | **Double-click to stop everything** |
| `CHECK_STATUS.bat` | Double-click to check status |
| `setup_autostart.ps1` | Install auto-start (run as admin) |
| `remove_autostart.ps1` | Uninstall auto-start (run as admin) |
| `monitor_and_update.ps1` | Main monitoring logic |
| `start_monitor.ps1` | Startup wrapper |
| `stop_all.ps1` | Stop all processes script |
| `monitor.log` | Activity log |
| `startup_error.log` | Startup errors (if any) |
| `TROUBLESHOOTING.md` | **Detailed troubleshooting guide** |

## ⚙️ Configuration

Edit `monitor_and_update.ps1` to change:

```powershell
$CHECK_INTERVAL = 600  # Update check frequency (seconds)
$GIT_CHECK_INTERVAL = 60  # Git status check frequency (seconds)
```

## 💡 Tips

1. **Always check logs first** when troubleshooting
2. **Keep monitor.log open** during testing with `Get-Content monitor.log -Wait`
3. **Test manual start first** before setting up auto-start
4. **Make sure .env is not committed** to git (it's in .gitignore)
5. **Telegram notifications** confirm everything is working

## Need More Help?

- **Troubleshooting:** See `TROUBLESHOOTING.md` for detailed solutions
- **Full Documentation:** See `README_CICD.md` for complete guide
- **Your Issues:**
  - Git fetch failed → Check git remote configuration
  - Telegram not working → Check .env file format
