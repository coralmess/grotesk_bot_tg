# 📁 CICD Folder - CI/CD Automation System

This folder contains all the automation scripts and documentation for the GroteskBotTg CI/CD system.

## 📜 Quick Reference

### PowerShell Scripts (Core System)

| Script | Purpose | How to Use |
|--------|---------|------------|
| `monitor_and_update.ps1` | Main monitoring engine | Called automatically by start_monitor.ps1 |
| `start_monitor.ps1` | Startup wrapper | Called by START_BOT.bat |
| `stop_all.ps1` | Stop all processes | Called by STOP_ALL.bat |
| `check_status.ps1` | System status checker | Called by CHECK_STATUS.bat |
| `setup_autostart.ps1` | Install auto-start | Right-click → Run as Administrator |
| `remove_autostart.ps1` | Remove auto-start | Right-click → Run as Administrator |

### Documentation Files

| File | What's Inside |
|------|---------------|
| **`SETUP_SUMMARY.md`** | 📘 **START HERE** - Complete overview and setup guide |
| **`QUICKSTART_CICD.md`** | ⚡ Quick reference for daily use |
| **`TROUBLESHOOTING.md`** | 🔧 Solutions for common problems |
| **`FIX_YOUR_ISSUES.md`** | 🩹 Fixes for specific errors (git, telegram, etc.) |
| `README_CICD.md` | 📖 Detailed CI/CD documentation |

## 🚀 Getting Started

### First Time Setup

1. **Read:** `SETUP_SUMMARY.md` - Complete setup instructions
2. **Install:** Run `setup_autostart.ps1` as Administrator (for auto-start)
3. **Test:** Use `CHECK_STATUS.bat` from main folder

### Daily Use

- **Start bot:** `START_BOT.bat` (in main folder)
- **Stop bot:** `STOP_ALL.bat` (in main folder)  
- **Check status:** `CHECK_STATUS.bat` (in main folder)

## 📋 What Each Script Does

### monitor_and_update.ps1
The core monitoring system that:
- Runs your GroteskBotTg.py bot
- Checks for git updates every 10 minutes
- Auto-pulls and restarts on updates
- Monitors bot health and restarts if crashed
- Sends Telegram notifications
- Logs everything to monitor.log

### start_monitor.ps1
Safe startup wrapper that:
- Validates monitor script exists
- Handles errors gracefully
- Provides user-friendly error messages

### stop_all.ps1
Comprehensive stop script that:
- Stops scheduled task
- Kills all bot processes
- Kills all monitor processes
- Verifies everything stopped

### check_status.ps1
Diagnostic tool that shows:
- Python/Git installation status
- Scheduled task status
- Bot process status
- Log file status
- Git repository status
- Configuration status

### setup_autostart.ps1
Auto-start installer that:
- Creates Windows scheduled task
- Configures to run at startup
- Sets up proper permissions
- Allows immediate start

### remove_autostart.ps1
Auto-start remover that:
- Stops running task
- Removes scheduled task
- Clean uninstall

## 📚 Documentation Structure

### For New Users
1. Read `SETUP_SUMMARY.md` - Get full picture
2. Follow setup steps
3. Keep `QUICKSTART_CICD.md` handy for reference

### For Troubleshooting
1. Check `TROUBLESHOOTING.md` for your specific issue
2. If you saw errors at startup, see `FIX_YOUR_ISSUES.md`
3. Use `CHECK_STATUS.bat` to diagnose

### For Advanced Users
- `README_CICD.md` - Deep dive into CI/CD system
- Edit `monitor_and_update.ps1` - Customize behavior

## 🎯 Common Tasks

### Enable Auto-Start
```
Right-click: setup_autostart.ps1
Select: Run with PowerShell as Administrator
```

### Disable Auto-Start
```
Right-click: remove_autostart.ps1
Select: Run with PowerShell as Administrator
```

### Change Check Interval
Edit `monitor_and_update.ps1`:
```powershell
$CHECK_INTERVAL = 600  # 10 minutes (change this value)
```

### View System Status
From main folder:
```
Double-click: CHECK_STATUS.bat
```

## ⚠️ Important Notes

### Don't Run Scripts Directly
- Use the `.bat` files in the main folder instead
- They handle paths correctly

### Administrator Rights
Only needed for:
- `setup_autostart.ps1` (one-time setup)
- `remove_autostart.ps1` (one-time removal)

Normal operation does NOT require admin rights.

### File Paths
All scripts now correctly reference the main folder for:
- `GroteskBotTg.py`
- `monitor.log`
- `.env` file
- Git repository

## 🔍 File Organization

```
d:\Chrome\911\LystTgFirefox\
├── GroteskBotTg.py          # Main bot (in root)
├── config.py                 # Configuration (in root)
├── .env                      # Secrets (in root)
├── monitor.log               # Logs (in root)
├── START_BOT.bat            # User controls (in root)
├── STOP_ALL.bat             # User controls (in root)
├── CHECK_STATUS.bat         # User controls (in root)
├── README.md                 # Main readme (in root)
│
└── CICD/                     # This folder
    ├── *.ps1                 # PowerShell scripts
    └── *.md                  # Documentation
```

## 💡 Pro Tips

1. **Always use .bat files** from main folder, not scripts directly
2. **Check logs first** when troubleshooting: `monitor.log`
3. **Use CHECK_STATUS.bat** to diagnose issues
4. **Read TROUBLESHOOTING.md** for common solutions
5. **Scripts are path-aware** - they know they're in CICD folder

## 🆘 Need Help?

1. **Quick answers:** `QUICKSTART_CICD.md`
2. **Setup issues:** `SETUP_SUMMARY.md`
3. **Errors:** `TROUBLESHOOTING.md` or `FIX_YOUR_ISSUES.md`
4. **Deep dive:** `README_CICD.md`

## 📝 Version History

- **v1.0** - Initial CI/CD system
- **v1.1** - Organized into CICD folder
- Path-aware scripts that reference main folder correctly
- Clean separation of user controls and system internals

---

**Return to main folder for normal bot operations!**
