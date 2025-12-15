@echo off
REM Quick status check for GroteskBotTg
cd /d "%~dp0"
powershell -ExecutionPolicy Bypass -File "%~dp0CICD\check_status.ps1"
pause
