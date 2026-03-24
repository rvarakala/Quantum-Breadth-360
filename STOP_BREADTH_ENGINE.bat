@echo off
title Stop Breadth Engine
color 0C

echo.
echo  Stopping Breadth Engine servers...
echo.

:: Kill port 8001 (backend)
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":8001 " 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)

:: Kill port 3000 (frontend)
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":3000 " 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)

echo  Done. Both servers stopped.
echo.
timeout /t 2 /nobreak >nul
