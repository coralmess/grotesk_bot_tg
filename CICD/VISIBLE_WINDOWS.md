# 📺 Bot Windows Now Visible!

## Changes Made

### ✅ Bot Windows Are Now Visible
The bot now runs in a **visible console window** so you can see:
- Real-time logs from GroteskBotTg
- Status messages
- Error messages
- All output from the bot

**What changed:**
- `UseShellExecute = true` - Shows windows instead of hiding them
- You'll see a separate console window for the bot process

### ✅ Fixed Telegram UTF-8 Encoding
**Problem:** `Bad Request: text must be encoded in UTF-8`

**Solution:**
- Properly encode messages as UTF-8 bytes
- Remove emoji characters that cause encoding issues
- Use simple text instead of special characters

### ✅ Removed Special Characters
Replaced all emojis with simple text in Telegram notifications:
- ❌ → "Error:"
- ✅ → Simple confirmation messages
- 🔄 → "Bot Updated"
- ⚠️ → "Bot Crashed"

## 🎯 What You'll See Now

### When You Start the Bot
1. **Monitor window** - Shows monitoring logs (CICD/monitor_and_update.ps1)
2. **Bot window** - Shows GroteskBotTg.py output (Python console)
3. Both windows stay open and visible

### Telegram Notifications (Clean Text)
- **"Bot Started - PID: 12345"**
- **"Bot Updated Successfully - Pulled latest changes and restarted."**
- **"Bot Crashed - Exit code: 1 - Restarting..."**
- **"Error: Bot failed to start (exit code: 1)"**

## 📊 Monitoring Multiple Bots

### GroteskBotTg Window
You'll see:
- Scraping progress
- Items found
- Database operations
- Telegram sending status
- Any errors or warnings

### OLX Scraper Window
The `olx_scraper.py` is called from within GroteskBotTg, so its output appears in the same bot window.

### Monitor Window
You'll see:
- "Bot started successfully"
- "Checking for updates..."
- "Fetching from remote repository..."
- Update notifications

## 🔄 Managing Windows

### Minimize vs Close
- **Minimize** - Bot keeps running in background
- **Close window** - Kills the bot process

### If You Close Bot Window Accidentally
The monitor will detect the crash and:
1. Log: "Bot process has exited unexpectedly"
2. Send Telegram: "Bot Crashed - Restarting..."
3. Auto-restart the bot
4. New window appears

## 💡 Best Practices

### For Normal Operation
1. **Start:** `START_BOT.bat`
2. **Minimize** both windows to taskbar
3. Let them run in background
4. Check windows occasionally for status

### For Monitoring
1. Keep **Bot window** visible to see real-time activity
2. Watch for errors or unusual behavior
3. Monitor logs show update checks every 10 minutes

### For Troubleshooting
1. Keep **both windows** visible
2. Watch for error messages
3. Bot window shows detailed Python errors
4. Monitor window shows system-level issues

## 🛑 Stopping Everything

### Option 1: Close Windows
- Close bot window → Bot stops
- Close monitor window → Monitoring stops
- Simple but not clean shutdown

### Option 2: Use STOP_ALL.bat (Recommended)
- Gracefully stops all processes
- Proper cleanup
- Recommended method

```
Double-click: STOP_ALL.bat
```

### Option 3: Keep Running
- Just minimize windows
- Let bot run in background
- Monitor auto-manages everything

## 📝 Window Output Examples

### Monitor Window
```
[2025-12-13 15:30:00] [INFO] ========== GROTESK BOT MONITOR STARTED ==========
[2025-12-13 15:30:00] [INFO] Script Directory: D:\Chrome\911\LystTgFirefox
[2025-12-13 15:30:00] [INFO] Found Python: python (Python 3.10.11)
[2025-12-13 15:30:01] [INFO] Starting GroteskBotTg...
[2025-12-13 15:30:04] [INFO] Bot started successfully (PID: 12345)
[2025-12-13 15:30:04] [INFO] Telegram notification sent successfully
```

### Bot Window (GroteskBotTg)
```
24.11 15:30 [INFO] Scraping page 1 for country IT - Grotesk Shoes
24.11 15:30 [INFO] Found 50 items for IT - Grotesk Shoes
24.11 15:31 [GOOD] New item  🍄🍄🍄
24.11 15:31 [INFO] Sending message to Telegram...
```

## 🎨 Window Arrangement Tips

### Side by Side
- Monitor window: Left half of screen
- Bot window: Right half of screen
- Easy to see both at once

### Stacked
- Monitor window: Top half
- Bot window: Bottom half
- Save horizontal space

### Minimized
- Both windows to taskbar
- Click to check when needed
- Clean desktop

## 🔧 If Windows Don't Appear

### Check Task Manager
1. Press `Ctrl+Shift+Esc`
2. Look for `python.exe` processes
3. Look for `powershell.exe` with monitor script

### Restart Clean
```powershell
# Stop everything
.\STOP_ALL.bat

# Wait 5 seconds

# Start fresh
.\START_BOT.bat
```

### Check Logs
```powershell
Get-Content monitor.log -Tail 20
```

## ⚙️ Advanced: Hide Windows Again

If you prefer hidden windows (old behavior):

Edit `CICD\monitor_and_update.ps1`:
```powershell
# Change this line:
$psi.UseShellExecute = $true

# To:
$psi.UseShellExecute = $false

# And add these back:
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$psi.CreateNoWindow = $true
```

Then restart the bot.

## 📱 Telegram Notifications Still Work

You'll still receive Telegram notifications for:
- Bot started
- Bot updated
- Bot crashed
- Errors

Now with clean, UTF-8 safe messages!

---

**Enjoy watching your bots in action!** 🎉
