@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - data_server MASTER (Windows)
REM
REM  Explicitly starts the data_server master as its own standalone process,
REM  instead of relying on the implicit auto-launch-from-first-bot path
REM  (trader_bot.py's ensure_master_running, which spawns one using whichever
REM  Python interpreter happens to be running the launching bot). Run this
REM  FIRST, before starting the fleet (run-app.bat / bot.py autostart) - bots
REM  that find a master already listening just connect to it instead of each
REM  racing to spawn their own.
REM
REM  Refuses to start if something is already listening on HOST:PORT - if you
REM  hit that, a master (or a stray duplicate) is already running; don't
REM  start a second one. Two masters fighting over the same exchange budget
REM  is what caused the fleet-wide rate-limit storm this script exists to
REM  prevent.
REM
REM  HOST is the interface the master BINDS to, and is what decides whether
REM  other machines can reach it:
REM      set HOST=127.0.0.1   (default) local bots/subservers only
REM      set HOST=0.0.0.0     every interface - remote bots/subservers connect
REM      set HOST=10.0.0.5    that one interface only
REM  Neither port authenticates anything: whatever can reach PORT can steer
REM  what this master collects, and whatever can reach SUBSERVER_PORT can push
REM  candles into its cache and onto disk. Only bind wide on a trusted/private
REM  network, or firewall both ports. Bots on other machines then point at
REM  this host via system.json's "data_server_ip".
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-dataserver] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "CONFIG=%CONFIG%"
if "%CONFIG%"=="" set "CONFIG=user_data\configs_system\data_server.json"
set "EXCHANGE=%EXCHANGE%"
if "%EXCHANGE%"=="" set "EXCHANGE=hyperliquid"
set "HOST=%HOST%"
if "%HOST%"=="" set "HOST=127.0.0.1"
set "PORT=%PORT%"
if "%PORT%"=="" set "PORT=8720"
set "SUBSERVER_PORT=%SUBSERVER_PORT%"
if "%SUBSERVER_PORT%"=="" set "SUBSERVER_PORT=8721"
set "TIMEFRAMES=%TIMEFRAMES%"
if "%TIMEFRAMES%"=="" set "TIMEFRAMES=15m"

python -c "import socket,os,sys; h=os.environ.get('HOST','127.0.0.1'); h='127.0.0.1' if h in ('0.0.0.0','') else h; p=int(os.environ.get('PORT','8720')); s=socket.socket(); s.settimeout(1.5); r=s.connect_ex((h,p)); sys.exit(0 if r==0 else 1)"
if not errorlevel 1 (
    echo [run-dataserver] Something is already listening on %HOST%:%PORT% - refusing to start a second master.
    exit /b 1
)

echo [run-dataserver] config=%CONFIG% exchange=%EXCHANGE% host=%HOST% port=%PORT% subserver-port=%SUBSERVER_PORT% timeframes=%TIMEFRAMES%
REM No "endlocal"/anything after this line, deliberately - cmd.exe only
REM prompts "Terminate batch job (Y/N)?" on Ctrl+C when it thinks there's
REM more of the script left to run once the current command ends. With
REM this the true last line, Ctrl+C just quits.
python -m VulcanTrader.data_server ^
    --mode master ^
    -c "%CONFIG%" ^
    --host "%HOST%" ^
    --port %PORT% ^
    --subserver-port %SUBSERVER_PORT% ^
    --exchange "%EXCHANGE%" ^
    --timeframes %TIMEFRAMES% ^
    -v %*
