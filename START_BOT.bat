@echo off
REM Windows Batch file to start the GroteskBotTg monitor
REM This provides a simple double-click option for starting the monitor

echo Starting GroteskBotTg Monitor...
echo.

REM Get the directory where this batch file is located
cd /d "%~dp0"

REM Run the PowerShell startup script
powershell -ExecutionPolicy Bypass -File "%~dp0CICD\start_monitor.ps1"

pause
