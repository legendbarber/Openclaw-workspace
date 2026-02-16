@echo off
setlocal
echo Stopping StockRadar windows/processes...

rem Kill windows started by START script (if titles match)
taskkill /FI "WINDOWTITLE eq StockRadar-App*" /T /F >nul 2>&1
taskkill /FI "WINDOWTITLE eq StockRadar-Tunnel*" /T /F >nul 2>&1

rem Fallback kills (safe enough for this dedicated use)
taskkill /IM cloudflared.exe /F >nul 2>&1
for /f "tokens=2" %%p in ('tasklist ^| findstr /I python.exe') do (
  wmic process where "ProcessId=%%p and CommandLine like '%%app.py%%'" get ProcessId /value 2>nul | find "=" >nul && taskkill /PID %%p /F >nul 2>&1
)

echo Done. If something is still running, close remaining cmd windows manually.
pause
