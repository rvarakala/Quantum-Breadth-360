@echo off
setlocal enabledelayedexpansion
title Quantum Breadth 360
color 0A

echo.
echo  ██████╗ ██████╗ ███████╗ █████╗ ██████╗ ████████╗██╗  ██╗
echo  ██╔══██╗██╔══██╗██╔════╝██╔══██╗██╔══██╗╚══██╔══╝██║  ██║
echo  ██████╔╝██████╔╝█████╗  ███████║██║  ██║   ██║   ███████║
echo  ██╔══██╗██╔══██╗██╔══╝  ██╔══██║██║  ██║   ██║   ██╔══██║
echo  ██████╔╝██║  ██║███████╗██║  ██║██████╔╝   ██║   ██║  ██║
echo  ╚═════╝ ╚═╝  ╚═╝╚══════╝╚═╝  ╚═╝╚═════╝    ╚═╝   ╚═╝  ╚═╝
echo.
echo   360  ^|  Market Intelligence Platform  ^|  localhost:8001
echo  ═══════════════════════════════════════════════════════════
echo.

cd /d "%~dp0backend"

:: Create .env from template if missing
if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo  [SETUP] .env created — edit it to add your GROQ_API_KEY
        echo.
    )
)

:: Create venv on first run
if not exist "venv\Scripts\activate.bat" (
    echo  [SETUP] First-time setup: creating virtual environment...
    python -m venv venv
    if errorlevel 1 ( echo  [ERROR] Python not found. Download from python.org & pause & exit /b 1 )
    echo  [SETUP] Installing packages (2-3 min first time)...
    call venv\Scripts\pip install -r requirements.txt --quiet
    echo  [SETUP] Done!
    echo.
)

call venv\Scripts\activate.bat

:: Kill anything on port 8001
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8001 "') do (
    taskkill /PID %%a /F >nul 2>&1
)

:: Start backend
echo  [START] Starting Quantum Breadth 360...
start /min "QB360-Backend" cmd /c "cd /d "%~dp0backend" && call venv\Scripts\activate.bat && python main.py"

:: Wait for server to be ready
echo  [START] Waiting for server...
timeout /t 4 /nobreak >nul

:: Open browser
echo  [START] Opening browser → http://localhost:8001
start "" "http://localhost:8001"

echo.
echo  ────────────────────────────────────────
echo   Quantum Breadth 360 is running!
echo   Dashboard: http://localhost:8001
echo   Server:    Running in background
echo.
echo   To stop: close the QB360-Backend window
echo  ────────────────────────────────────────
echo.
pause
