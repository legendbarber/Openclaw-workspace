@echo off
setlocal
cd /d C:\Users\mangi\.openclaw\workspace

echo [1/2] Starting Flask app on http://localhost:3000 ...
start "StockRadar-App" cmd /k "cd /d C:\Users\mangi\.openclaw\workspace && python app.py"

timeout /t 2 /nobreak >nul

echo [2/2] Starting Cloudflare Quick Tunnel...
echo.
echo IMPORTANT: Copy the https://*.trycloudflare.com URL shown in the Tunnel window.
echo Keep BOTH windows open while using the service.
echo.
start "StockRadar-Tunnel" cmd /k "cloudflared tunnel --url http://localhost:3000"

echo Done.
echo - Local:  http://localhost:3000
echo - Remote: check the 'StockRadar-Tunnel' window for the trycloudflare URL
echo.
pause
