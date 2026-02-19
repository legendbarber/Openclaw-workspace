$ErrorActionPreference = 'SilentlyContinue'

$workspace = 'C:\Users\mangi\.openclaw\workspace'
$investDir = Join-Path $workspace 'web\invest-recommand'
$temaDir = Join-Path $workspace 'web\temaWeb-v2'
$python = 'C:\Windows\py.exe'
$uvicorn = Join-Path $temaDir '.venv311\Scripts\uvicorn.exe'

function Start-Invest {
  $listen = Get-NetTCPConnection -State Listen -LocalPort 3000 -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $listen) {
    Start-Process -FilePath $python -ArgumentList '-3','app.py' -WorkingDirectory $investDir -WindowStyle Hidden
  }
}

function Start-Tema {
  $listen = Get-NetTCPConnection -State Listen -LocalPort 3010 -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $listen) {
    $cmd = 'set ENABLE_REFRESH=true&& "' + $uvicorn + '" app:app --host 127.0.0.1 --port 3010'
    Start-Process -FilePath 'C:\Windows\System32\cmd.exe' -ArgumentList '/c', $cmd -WorkingDirectory $temaDir -WindowStyle Hidden
  }
}

Start-Invest
Start-Tema
