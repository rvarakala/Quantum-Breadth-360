@echo off
title Quantum Breadth 360 - One Click Launch
color 0A
mode con: cols=70 lines=35

echo.
echo  ====================================================
echo.
echo   ____  ____  _____    _    ____  _____ _   _
echo  ^| __ )^|  _ \^| ____|  / \  ^|  _ \^|_   _^| ^| ^| ^|
echo  ^|  _ \^| ^|_) ^|  _^|   / _ \ ^| ^| ^| ^| ^| ^| ^| ^|_^| ^|
echo  ^| ^|_) ^|  _ ^<^| ^|___ / ___ \^| ^|_^| ^| ^| ^| ^|  _  ^|
echo  ^|____/^|_^| \_\_____/_/   \_\____/  ^|_^| ^|_^| ^|_^|
echo.
echo                    3 6 0
echo.
echo  ====================================================
echo   Market Breadth Engine - One Click Launch
echo  ====================================================
echo.

:: ── Configuration ─────────────────────────────────────
set APP_DIR=C:\QUANTUM_BREADTH_360
set VENV_DIR=%APP_DIR%\venv
set BACKEND_DIR=%APP_DIR%\backend
set PORT=8001
set URL=http://localhost:%PORT%

:: ── Step 0: Navigate to app directory ─────────────────
cd /d %APP_DIR% 2>nul
if errorlevel 1 (
    echo  [ERROR] App directory not found: %APP_DIR%
    echo  Please update APP_DIR in this bat file.
    pause & exit /b
)

:: ── Step 1: Activate virtual environment ──────────────
echo  [1/5] Activating Python environment...
if exist "%VENV_DIR%\Scripts\activate.bat" (
    call "%VENV_DIR%\Scripts\activate.bat"
) else (
    echo  [WARN] No venv found. Using system Python.
)

python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python not found! Install Python 3.9+
    pause & exit /b
)
echo        Done.
echo.

:: ── Step 2: Install/update packages if needed ─────────
echo  [2/5] Checking dependencies...
python -c "import fastapi" >nul 2>&1
if errorlevel 1 (
    echo        Installing packages...
    pip install -r backend\requirements.txt --quiet
)
echo        Done.
echo.

:: ── Step 3: Kill any old process on port ──────────────
echo  [3/5] Clearing port %PORT%...
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%PORT% " 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 1 /nobreak >nul
echo        Done.
echo.

:: ── Step 4: Update data (incremental daily sync) ──────
echo  [4/5] Updating market data...
echo        Fetching latest OHLCV data from yfinance...
echo        (This may take 2-5 minutes on first run)
echo.

:: Run the daily update via the API sync mechanism
:: Start server temporarily for the sync
start /min "" cmd /c "cd /d %APP_DIR% && call %VENV_DIR%\Scripts\activate.bat && cd backend && python main.py"
echo        Waiting for server to start...
timeout /t 8 /nobreak >nul

:: Trigger daily update via API
echo        Triggering data sync...
curl -s -X POST "%URL%/api/sync/update" >nul 2>&1
if errorlevel 1 (
    echo        [INFO] curl not available - using PowerShell...
    powershell -Command "try { Invoke-RestMethod -Uri '%URL%/api/sync/update' -Method POST | Out-Null } catch {}" >nul 2>&1
)

:: Wait for sync to complete (poll status)
echo        Syncing data...
set SYNC_DONE=0
set SYNC_COUNT=0

:sync_loop
if %SYNC_COUNT% GEQ 60 goto sync_timeout
timeout /t 5 /nobreak >nul
set /a SYNC_COUNT+=1

:: Check sync status
for /f "delims=" %%r in ('curl -s "%URL%/api/sync/status" 2^>nul') do set SYNC_RESULT=%%r
echo %SYNC_RESULT% | findstr /i "running" >nul 2>&1
if errorlevel 1 (
    set SYNC_DONE=1
    goto sync_done
)
echo        Still syncing... (%SYNC_COUNT%/60)
goto sync_loop

:sync_timeout
echo        [WARN] Sync taking too long - continuing with cached data.
goto sync_continue

:sync_done
echo        Data sync complete!

:sync_continue
echo.

:: ── Step 5: Clear cache and restart fresh ─────────────
echo  [5/5] Starting Quantum Breadth 360...

:: Kill the temp server
for /f "tokens=5" %%a in ('netstat -aon ^| findstr ":%PORT% " 2^>nul') do (
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 2 /nobreak >nul

:: Start the final server
start /min "Breadth360-Server" cmd /c "cd /d %APP_DIR% && call %VENV_DIR%\Scripts\activate.bat && cd backend && python main.py"
timeout /t 5 /nobreak >nul

:: Open browser
start "" %URL%

echo.
echo  ====================================================
echo.
echo   BREADTH 360 IS RUNNING!
echo.
echo   Dashboard : %URL%
echo   Server    : Running in background
echo.
echo   Data is up to date.
echo.
echo   To stop: Close this window, then run
echo            STOP_BREADTH_ENGINE.bat
echo.
echo  ====================================================
echo.
echo  Press any key to keep this window open...
echo  (Server keeps running even if you close this)
echo.
pause >nul
