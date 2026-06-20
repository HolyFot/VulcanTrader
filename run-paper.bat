@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - paper / dry-run trading (Windows)
REM
REM  Starts the trader bot in --dry-run mode. The trade subcommand also
REM  brings up the FastAPI web portal automatically (pass --no-web to skip).
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-paper] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "CONFIG=%CONFIG%"
if "%CONFIG%"=="" set "CONFIG=configMyStrategy_Paper"

set "STRATEGY=%STRATEGY%"
if "%STRATEGY%"=="" set "STRATEGY=MyStrategy"

set "DB_URL=%DB_URL%"
if "%DB_URL%"=="" set "DB_URL=json:///user_data/accounts/MyStrategy_dry_run.json"

echo [run-paper] config=%CONFIG%  strategy=%STRATEGY%  db=%DB_URL%
python -m VulcanTrader.bot trade ^
    -v ^
    --dry-run ^
    -c "%CONFIG%" ^
    --strategy "%STRATEGY%" ^
    --db-url "%DB_URL%" %*

endlocal
