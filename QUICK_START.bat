@echo off
title Quantum Breadth 360 - Quick Start (No Data Update)
color 0A

echo.
echo  ============================================
echo   BREADTH 360 - Quick Start
echo   (Using cached data - no sync)
echo  ============================================
echo.

set APP_DIR=C:\QUANTUM_BREADTH_360
set PORT=8001

cd /d %APP_DIR% 2>nul || (echo [ERROR] %APP_DIR% not found & pause & exit /b)

:: Activate venv
if exist venv\Scripts\activate.bat call venv\Scripts\activate.bat

:: Kill old process
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%PORT% " 2^>nul') do taskkill /PID %%a /F >nul 2>&1
timeout /t 1 /nobreak >nul

:: Start server
echo  Starting server...
start /min "Breadth360" cmd /c "cd /d %APP_DIR% && call venv\Scripts\activate.bat && cd backend && python main.py"
timeout /t 4 /nobreak >nul

:: Open browser
start "" http://localhost:%PORT%

echo.
echo  RUNNING! Dashboard: http://localhost:%PORT%
echo.
echo  Press any key to exit (server stays running)
pause >nul
