@echo off
title Breadth Engine - Data Import
color 0B

cd /d C:\breadth-app
call venv\Scripts\activate.bat

echo.
echo  ============================================
echo   BREADTH ENGINE - DATA IMPORTER
echo  ============================================
echo.
echo  [1] Import NIFTY 500 ticker list
echo  [2] Import Sectors CSV
echo  [3] Import NSE CSV data (30 years)
echo  [4] Check database status
echo  [5] Exit
echo.
set /p choice="  Enter choice (1-5): "

if "%choice%"=="1" goto import_tickers
if "%choice%"=="2" goto import_sectors
if "%choice%"=="3" goto import_csv
if "%choice%"=="4" goto db_status
if "%choice%"=="5" exit /b
goto end

:import_tickers
echo.
echo  Importing NIFTY 500 ticker list...
python db_tool.py import-nifty backend\data\nifty500_clean.csv
echo.
goto end

:import_sectors
echo.
set /p sf="  Enter full path to sectors.csv: "
python db_tool.py import-sectors "%sf%"
echo.
goto end

:import_csv
echo.
echo  Importing NSE historical CSV data...
echo  This takes 5-10 minutes for 30 years of data.
echo.
python backend\import_local.py
echo.
goto end

:db_status
echo.
echo  Database Status:
python db_tool.py status
echo.
goto end

:end
echo.
pause
