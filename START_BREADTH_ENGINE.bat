@echo off
title Quantum Breadth 360 — Backend Server
echo.
echo  ██████  ██    ██  █████  ███    ██ ████████ ██    ██ ███    ███
echo ██    ██ ██    ██ ██   ██ ████   ██    ██    ██    ██ ████  ████
echo ██    ██ ██    ██ ███████ ██ ██  ██    ██    ██    ██ ██ ████ ██
echo ██ ▄▄ ██ ██    ██ ██   ██ ██  ██ ██    ██    ██    ██ ██  ██  ██
echo  ██████   ██████  ██   ██ ██   ████    ██     ██████  ██      ██
echo.
echo  BREADTH 360 — Market Intelligence Platform
echo  ==========================================
echo.

cd /d "%~dp0backend"

:: Check if venv exists
if not exist "venv\Scripts\activate.bat" (
    echo [SETUP] Creating virtual environment...
    python -m venv venv
    echo [SETUP] Installing dependencies...
    venv\Scripts\pip install -r requirements.txt
    echo [SETUP] Setup complete!
    echo.
)

:: Create .env from example if not exists
if not exist ".env" (
    if exist ".env.example" (
        copy ".env.example" ".env" >nul
        echo [SETUP] Created .env file from template
        echo [SETUP] Edit .env and add your GROQ_API_KEY for AI features
        echo.
    )
)

:: Activate venv and start
call venv\Scripts\activate.bat
echo [START] Starting Quantum Breadth 360 backend...
echo [START] Open browser: http://localhost:8001
echo.
python main.py

pause
