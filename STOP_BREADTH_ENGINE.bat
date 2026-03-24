@echo off
echo Stopping Quantum Breadth 360...
for /f "tokens=5" %%a in ('netstat -ano ^| findstr :8001') do (
    taskkill /PID %%a /F >nul 2>&1
)
echo Done.
