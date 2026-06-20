@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - web dashboard only (Windows)
REM
REM  Starts the FastAPI web portal WITHOUT starting live or paper trading.
REM  Use this to browse backtest results, inspect closed trades, and use
REM  the analysis tools when no bot session is running.
REM
REM  Dashboard: http://localhost:8080
REM  Default password: VulcanTrader
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-app] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "CONFIG=%CONFIG%"
if "%CONFIG%"=="" set "CONFIG=configHyper"

set "PORT=%PORT%"
if "%PORT%"=="" set "PORT=8080"

echo [run-app] config=%CONFIG%  port=%PORT%
python -m VulcanTrader.bot webserver ^
    -v ^
    -c "%CONFIG%" ^
    --port "%PORT%" %*

endlocal
