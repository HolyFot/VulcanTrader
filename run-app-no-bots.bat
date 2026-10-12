@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - web dashboard only, no auto-started bots (Windows)
REM
REM  Starts the FastAPI web portal WITHOUT launching any auto_start: true
REM  configs (passes --no-autostart to `webserver`). Use this to browse
REM  backtest results, inspect closed trades, and use the analysis tools
REM  without bringing up the trading fleet.
REM
REM  Dashboard: http://localhost:8060
REM  Default password: test
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-app-no-bots] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "PORT=%PORT%"
if "%PORT%"=="" set "PORT=8060"

echo [run-app-no-bots] port=%PORT%
REM No "endlocal"/anything after this line, deliberately - cmd.exe only
REM prompts "Terminate batch job (Y/N)?" on Ctrl+C when it thinks there's
REM more of the script left to run once the current command ends. With
REM this the true last line, Ctrl+C just quits.
python -m VulcanTrader.bot webserver ^
    -v ^
    --no-autostart ^
    --port "%PORT%" %*
