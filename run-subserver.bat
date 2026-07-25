@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - data_server subserver (Windows)
REM
REM  Runs a data_server subserver: dials out to a data_server MASTER (any
REM  `bot.py trade`/`bot.py autostart` instance auto-launches one if none is
REM  running yet) and takes on a share of its OHLCV/funding-rate/trades
REM  collection work. The master never
REM  launches a subserver itself - it's a deliberate, separately-run process,
REM  typically on a different machine.
REM
REM  data_server has ONE general config per machine (not one per exchange) at
REM  user_data\configs_system\data_server.json. On a dedicated subserver
REM  machine, copy user_data\configs_system\config.subserver.example.json
REM  to that path and set "master_host" to the real master's IP. --mode is
REM  deliberately omitted below: the config's "is_subserver": true already
REM  selects subserver mode on its own.
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-subserver] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "CONFIG=%CONFIG%"
if "%CONFIG%"=="" set "CONFIG=user_data\configs_system\data_server.json"

echo [run-subserver] config=%CONFIG%
REM No "endlocal"/anything after this line, deliberately - cmd.exe only
REM prompts "Terminate batch job (Y/N)?" on Ctrl+C when it thinks there's
REM more of the script left to run once the current command ends. With
REM this the true last line, Ctrl+C just quits.
python -m VulcanTrader.data_server ^
    -v ^
    -c "%CONFIG%" %*