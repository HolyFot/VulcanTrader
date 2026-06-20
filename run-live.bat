@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - LIVE trading (real money) (Windows)
REM
REM  Starts the trader bot WITHOUT --dry-run. The trade subcommand also
REM  brings up the FastAPI web portal automatically (pass --no-web to skip).
REM
REM  WARNING: this will place real orders against the configured exchange.
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-live] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "CONFIG=%CONFIG%"
if "%CONFIG%"=="" set "CONFIG=configMyStrategy_Live"

set "STRATEGY=%STRATEGY%"
if "%STRATEGY%"=="" set "STRATEGY=MyStrategy"

set "DB_URL=%DB_URL%"
if "%DB_URL%"=="" set "DB_URL=json:///user_data/accounts/MyStrategy_live.json"

echo.
echo  ============================================================
echo   LIVE TRADING - real orders will be placed.
echo   config=%CONFIG%  strategy=%STRATEGY%  db=%DB_URL%
echo  ============================================================

python -m VulcanTrader.bot trade ^
    -v ^
    -c "%CONFIG%" ^
    --strategy "%STRATEGY%" ^
    --db-url "%DB_URL%" %*

endlocal
