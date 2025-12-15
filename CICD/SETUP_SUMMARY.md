# 🤖 GroteskBotTg CI/CD System - Complete Setup

## 📦 What Was Created

I've set up a complete CI/CD system for your GroteskBotTg on Windows 10. Here's what you now have:

### Core Scripts
1. **monitor_and_update.ps1** - Main monitor that:
   - Checks for git updates every 10 minutes
   - Auto-pulls and restarts bot when updates found
   - Monitors bot health and restarts if crashed
   - Sends Telegram notifications for all events
   - Logs everything to `monitor.log`

2. **start_monitor.ps1** - Startup wrapper with error handling

3. **START_BOT.bat** - Simple double-click launcher for manual start

### Setup & Management
4. **setup_autostart.ps1** - Creates Windows Task Scheduler entry (run as admin)
5. **remove_autostart.ps1** - Removes auto-start task (run as admin)
6. **check_status.ps1** - Shows system status and diagnostics

### Documentation
7. **README_CICD.md** - Complete detailed documentation
8. **QUICKSTART_CICD.md** - Quick reference guide
9. **SETUP_SUMMARY.md** - This file

### Configuration
10. Updated **.gitignore** - Excludes log files from git

---

## 🚀 Quick Setup (Choose One)

### Option A: Auto-Start on Windows Boot ⭐ RECOMMENDED
```
1. Right-click "setup_autostart.ps1"
2. Select "Run with PowerShell as Administrator"
3. Follow prompts
4. Done! Bot starts automatically on every boot
```

### Option B: Manual Start (For Testing)
```
1. Double-click "START_BOT.bat"
2. Bot runs while window is open
3. Close window to stop
```

---

## 🔄 How Auto-Update Works

### Monitoring Cycle
- **Every 60 seconds**: Check if local code is behind remote
- **Every 10 minutes**: Fetch from git to get latest commits
- **On update detected**: Auto-pull, restart bot, send notification

### Update Sequence (Automatic)
1. 🛑 Stop bot gracefully
2. 📥 Pull latest code from git
3. 📦 Install updated dependencies
4. ✅ Restart bot with new code
5. 📱 Send Telegram notification

### Health Monitoring
- Continuously checks if bot is running
- Auto-restarts if bot crashes
- Sends notifications on crashes

---

## 📱 Telegram Notifications

You'll receive notifications to `DANYLO_DEFAULT_CHAT_ID` for:

| Event | Message |
|-------|---------|
| Bot started | ✅ **Bot Started** |
| Updates applied | 🔄 **Bot Updated Successfully** |
| Bot crashed | ⚠️ **Bot Crashed** (auto-restarting) |
| Errors | ❌ **Error: [details]** |

---

## 📊 Monitoring & Logs

### View Real-Time Logs
```powershell
# PowerShell - Live view
cd "d:\Chrome\911\LystTgFirefox"
Get-Content monitor.log -Wait
```

### Check System Status
```powershell
# PowerShell - Run status check
cd "d:\Chrome\911\LystTgFirefox"
.\check_status.ps1
```

### Manage Task
```powershell
# View in Task Scheduler
Win+R → taskschd.msc → Find "GroteskBotTg-AutoStart"

# Stop task
Stop-ScheduledTask -TaskName "GroteskBotTg-AutoStart"

# Start task
Start-ScheduledTask -TaskName "GroteskBotTg-AutoStart"
```

---

## 🛠️ Common Operations

### Check if Bot is Running
```powershell
.\check_status.ps1
```

### View Recent Logs
```powershell
Get-Content monitor.log -Tail 20
```

### Manually Update Bot
```powershell
git pull origin master
pip install -r requirements.txt
# Then restart task or START_BOT.bat
```

### Disable Auto-Start
```powershell
# Right-click remove_autostart.ps1 → Run as Admin
```

### Re-enable Auto-Start
```powershell
# Right-click setup_autostart.ps1 → Run as Admin
```

---

## ✅ Pre-Requirements Checklist

Before starting, ensure:
- [ ] Python 3.x installed and in PATH (`python --version`)
- [ ] Git installed and in PATH (`git --version`)
- [ ] Git repository initialized with remote configured
- [ ] `.env` file exists with:
  - [ ] `TELEGRAM_BOT_TOKEN=your_token`
  - [ ] `DANYLO_DEFAULT_CHAT_ID=your_chat_id`
- [ ] `config.py` properly configured
- [ ] `requirements.txt` exists

Test with:
```powershell
python --version
git --version
git remote -v
```

---

## 🎯 What Happens Now

### With Auto-Start Enabled:
1. **On Windows Boot**: Bot automatically starts
2. **Every 10 min**: Checks for updates
3. **On Update**: Auto-pulls, restarts, notifies you
4. **On Crash**: Auto-restarts, notifies you
5. **Always**: Logs everything to `monitor.log`

### With Manual Start:
- Run `START_BOT.bat` when you want bot running
- Same monitoring and auto-update features
- Stops when you close window

---

## 📁 File Structure

```
d:\Chrome\911\LystTgFirefox\
├── GroteskBotTg.py              # Your bot
├── config.py                     # Config file
├── .env                          # Secrets (not in git)
├── requirements.txt              # Dependencies
│
├── START_BOT.bat                 # 👈 Double-click to start
├── monitor_and_update.ps1        # Main monitor logic
├── start_monitor.ps1             # Startup wrapper
│
├── setup_autostart.ps1           # 👈 Run as Admin to enable auto-start
├── remove_autostart.ps1          # 👈 Run as Admin to disable auto-start
├── check_status.ps1              # 👈 Check system status
│
├── README_CICD.md                # 📖 Full documentation
├── QUICKSTART_CICD.md            # 📖 Quick reference
├── SETUP_SUMMARY.md              # 📖 This file
│
├── monitor.log                   # Activity log (auto-created)
└── startup_error.log             # Error log (if errors occur)
```

---

## 🆘 Troubleshooting

### Bot Won't Start
1. Run `.\check_status.ps1` to diagnose
2. Check `monitor.log` for errors
3. Verify Python/Git are in PATH
4. Test manually: `python GroteskBotTg.py`

### No Auto-Updates
1. Check git remote: `git remote -v`
2. Verify internet connection
3. Check `monitor.log` for git errors
4. Test manually: `git fetch origin`

### No Telegram Notifications
1. Verify `.env` has `TELEGRAM_BOT_TOKEN` and `DANYLO_DEFAULT_CHAT_ID`
2. Check `monitor.log` for notification errors
3. Test bot token manually

### Task Doesn't Run
1. Verify ran `setup_autostart.ps1` as Administrator
2. Check Task Scheduler: `Win+R` → `taskschd.msc`
3. Look for "GroteskBotTg-AutoStart" task
4. Check task history for errors

---

## 💡 Pro Tips

1. **Always check logs first** when troubleshooting
2. **Use `check_status.ps1`** to get full system status
3. **Test manual start first** before enabling auto-start
4. **Monitor logs in real-time** during testing: `Get-Content monitor.log -Wait`
5. **Keep `.env` secure** - never commit to git
6. **Regular commits** - monitor will auto-pull your updates

---

## 🎓 Next Steps

### Immediate
1. ✅ Run `.\check_status.ps1` to verify all requirements
2. ✅ Test manual start with `START_BOT.bat`
3. ✅ Check that Telegram notifications work
4. ✅ Review `monitor.log`

### For Production
1. ✅ Run `setup_autostart.ps1` as Administrator
2. ✅ Verify task shows in Task Scheduler
3. ✅ Test full cycle: make a git commit, push, wait 10 min
4. ✅ Confirm you receive update notification

---

## 📚 Documentation Reference

- **QUICKSTART_CICD.md** - Quick reference for daily use
- **README_CICD.md** - Complete detailed documentation
- **This file** - Setup summary and overview

---

## 🎉 You're All Set!

Your GroteskBotTg now has:
- ✅ Auto-start on Windows boot
- ✅ Auto-update from git every 10 minutes
- ✅ Auto-restart on crashes
- ✅ Telegram notifications for all events
- ✅ Comprehensive logging
- ✅ Easy management tools

**Start testing with:** `START_BOT.bat` or setup auto-start!

Questions? Check the documentation or review the logs!
