@echo off
title Quantum Breadth 360 — Stop
color 0C

echo.
echo  [STOP] Stopping Quantum Breadth 360...
echo.

set KILLED=0
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":8001 "') do (
    echo  [STOP] Killing process %%a on port 8001...
    taskkill /PID %%a /F >nul 2>&1
    set KILLED=1
)

if %KILLED%==1 (
    echo  [DONE] Server stopped successfully.
) else (
    echo  [INFO] No server was running on port 8001.
)

echo.
pause
