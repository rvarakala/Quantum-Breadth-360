@echo off
setlocal enabledelayedexpansion
title Quantum Breadth 360 — Restart
color 0E

echo.
echo  ╔══════════════════════════════════════════╗
echo  ║   QUANTUM BREADTH 360 — RESTART          ║
echo  ╚══════════════════════════════════════════╝
echo.

cd /d "%~dp0backend"

:: Step 1: Kill existing server
echo  [1/4] Stopping server...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8001 "') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo        Done.

:: Step 2: Pull latest code
echo.
echo  [2/4] Pulling latest code from GitHub...
cd /d "%~dp0"
git pull
cd /d "%~dp0backend"

:: Step 3: Check/install dependencies
if not exist "venv\Scripts\activate.bat" (
    echo  [SETUP] Creating virtual environment...
    python -m venv venv
    call venv\Scripts\pip install -r requirements.txt --quiet
)
call venv\Scripts\activate.bat
echo.
echo  [3/4] Checking dependencies...
pip install -r requirements.txt --quiet

:: Step 4: Start server + open browser
echo.
echo  [4/4] Starting Quantum Breadth 360...
start /min "QB360-Backend" cmd /c "cd /d "%~dp0backend" && call venv\Scripts\activate.bat && python main.py"

timeout /t 4 /nobreak >nul
start "" "http://localhost:8001"

echo.
echo  ────────────────────────────────────────
echo   Restarted! Dashboard: http://localhost:8001
echo   Press any key to close this window
echo  ────────────────────────────────────────
echo.
pause
