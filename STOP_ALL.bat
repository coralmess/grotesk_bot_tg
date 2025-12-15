@echo off
REM Stop all GroteskBotTg processes
echo Stopping all GroteskBotTg processes...
echo.

cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0CICD\stop_all.ps1"

pause
