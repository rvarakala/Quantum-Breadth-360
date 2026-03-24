@echo off
setlocal enabledelayedexpansion
title Quantum Breadth 360 — Backend Server
color 0A

echo.
echo  ██████╗ ██╗   ██╗ █████╗ ███╗   ██╗████████╗██╗   ██╗███╗   ███╗
echo ██╔═══██╗██║   ██║██╔══██╗████╗  ██║╚══██╔══╝██║   ██║████╗ ████║
echo ██║   ██║██║   ██║███████║██╔██╗ ██║   ██║   ██║   ██║██╔████╔██║
echo ██║▄▄ ██║██║   ██║██╔══██║██║╚██╗██║   ██║   ██║   ██║██║╚██╔╝██║
echo ╚██████╔╝╚██████╔╝██║  ██║██║ ╚████║   ██║   ╚██████╔╝██║ ╚═╝ ██║
echo  ╚══▀▀═╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═══╝   ╚═╝    ╚═════╝ ╚═╝     ╚═╝
echo.
echo  BREADTH 360  ^^^|  Market Intelligence Platform  ^^^|  localhost:8001
echo  ═══════════════════════════════════════════════════════════════════
echo.

:: ── Set working directory to script location ─────────────────────────────────
cd /d "%~dp0backend"

:: ── Create .env from template if not exists ───────────────────────────────────
if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo  [SETUP] .env created from template
        echo  [SETUP] Edit .env to add your GROQ_API_KEY for AI features
        echo.
    )
)

:: ── Create venv if not exists ─────────────────────────────────────────────────
if not exist "venv\Scripts\activate.bat" (
    echo  [SETUP] Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo  [ERROR] Failed to create venv. Is Python installed?
        echo  [ERROR] Download Python 3.11+ from python.org
        pause
        exit /b 1
    )
    echo  [SETUP] Installing dependencies ^(first time — takes 2-3 min^)...
    call venv\Scripts\pip install -r requirements.txt --quiet
    if errorlevel 1 (
        echo  [ERROR] pip install failed. Check requirements.txt
        pause
        exit /b 1
    )
    echo  [SETUP] Setup complete!
    echo.
)

:: ── Activate venv ─────────────────────────────────────────────────────────────
call venv\Scripts\activate.bat

:: ── Kill anything on port 8001 ────────────────────────────────────────────────
echo  [START] Checking port 8001...
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8001 "') do (
    taskkill /PID %%a /F >nul 2>&1
)

:: ── Start backend ─────────────────────────────────────────────────────────────
echo  [START] Launching Quantum Breadth 360...
echo  [START] Open browser ^-^> http://localhost:8001
echo.
echo  ─────────────────────────────────────────────
echo   Press Ctrl+C to stop the server
echo  ─────────────────────────────────────────────
echo.

python main.py

:: ── On exit ───────────────────────────────────────────────────────────────────
echo.
echo  [STOP] Server stopped.
pause
