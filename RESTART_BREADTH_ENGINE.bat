@echo off
setlocal enabledelayedexpansion
title Quantum Breadth 360 — Restart
color 0E

echo.
echo  ╔══════════════════════════════════════════╗
echo  ║   QUANTUM BREADTH 360 — RESTART          ║
echo  ╚══════════════════════════════════════════╝
echo.

:: ── Set working directory ────────────────────────────────────────────────────
cd /d "%~dp0backend"

:: ── Step 1: Kill existing server ─────────────────────────────────────────────
echo  [1/3] Stopping existing server on port 8001...
set KILLED=0
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8001 "') do (
    taskkill /PID %%a /F >nul 2>&1
    set KILLED=1
)
if !KILLED!==1 (
    echo        Server stopped.
) else (
    echo        No server was running.
)

:: ── Step 2: Pull latest from GitHub ──────────────────────────────────────────
echo.
echo  [2/3] Pulling latest code from GitHub...
cd /d "%~dp0"
git pull
if errorlevel 1 (
    echo  [WARN] git pull failed — starting with current code
)
cd /d "%~dp0backend"

:: ── Step 3: Start fresh ───────────────────────────────────────────────────────
echo.
echo  [3/3] Starting Quantum Breadth 360...
echo.

:: Activate venv
if not exist "venv\Scripts\activate.bat" (
    echo  [SETUP] Creating virtual environment...
    python -m venv venv
    call venv\Scripts\pip install -r requirements.txt --quiet
)
call venv\Scripts\activate.bat

:: Check for new dependencies
echo  [INFO] Checking dependencies...
pip install -r requirements.txt --quiet --upgrade

echo.
echo  ─────────────────────────────────────────────
echo   Quantum Breadth 360 restarting...
echo   Open browser -> http://localhost:8001
echo   Press Ctrl+C to stop
echo  ─────────────────────────────────────────────
echo.

python main.py

echo.
echo  [STOP] Server stopped.
pause
