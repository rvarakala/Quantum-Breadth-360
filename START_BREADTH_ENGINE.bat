@echo off
title Breadth Engine
color 0A

echo.
echo  ============================================
echo   BREADTH ENGINE - Starting up...
echo  ============================================
echo.

cd /d C:\QUANTUM_BREADTH_360
call venv\Scripts\activate.bat

python --version >nul 2>&1
if errorlevel 1 (
    echo  ERROR: Python not found.
    pause & exit /b
)

:: Install packages if missing
python -c "import fastapi" >nul 2>&1
if errorlevel 1 (
    echo  Installing packages...
    pip install fastapi uvicorn yfinance pandas numpy --quiet
)

:: Kill old process on port 8001
echo  Clearing port 8001...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8001 " 2^>nul') do taskkill /PID %%a /F >nul 2>&1
timeout /t 2 /nobreak >nul

:: Start backend (serves both API and frontend)
echo  Starting Breadth Engine...
start /min "" cmd /c "cd /d C:\QUANTUM_BREADTH_360 && call venv\Scripts\activate.bat && cd backend && python main.py"
timeout /t 5 /nobreak >nul

:: Open browser - everything on port 8001 now
echo  Opening dashboard...
cmd /c start "" http://localhost:8001
timeout /t 1 /nobreak >nul
powershell -Command "Start-Process 'http://localhost:8001'" >nul 2>&1

echo.
echo  ============================================
echo   RUNNING!
echo   Dashboard : http://localhost:8001
echo   (No separate frontend server needed!)
echo  ============================================
echo.
pause
