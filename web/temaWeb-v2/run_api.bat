@echo off
setlocal

REM Force working directory to this script's folder
cd /d "%~dp0"

REM Settings (ASCII only to avoid encoding issues)
set "TEMA_ROOT=C:\project\04.app\temaWeb\tema"
set "ENABLE_REFRESH=true"
set "CRAWLER_PATH=%~dp0crawler\01today_tema.py"

REM Prefer venv python if exists
set "PYEXE=python"
if exist "%~dp0.venv\Scripts\python.exe" set "PYEXE=%~dp0.venv\Scripts\python.exe"

echo [BAT] CWD=%CD%
echo [BAT] PYEXE=%PYEXE%
echo [BAT] ENABLE_REFRESH=%ENABLE_REFRESH%
echo [BAT] TEMA_ROOT=%TEMA_ROOT%
echo [BAT] CRAWLER_PATH=%CRAWLER_PATH%
%PYEXE% -c "import os; print('[PY] ENABLE_REFRESH=', os.getenv('ENABLE_REFRESH')); print('[PY] TEMA_ROOT=', os.getenv('TEMA_ROOT'))"

echo Starting API on http://0.0.0.0:8000
%PYEXE% -m uvicorn app:app --host 0.0.0.0 --port 8000
