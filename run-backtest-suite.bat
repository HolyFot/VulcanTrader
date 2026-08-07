@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - rust-engine backtest suite (Windows)
REM
REM  Two-phase pipeline, auto-discovering one "main" strategy per top-level
REM  .py file directly under user_data\strategies (subdirectories like New\,
REM  OLD\, examples\ are skipped - see VulcanTrader\util\run_backtest_suite.py's
REM  find_primary_strategies for exactly how):
REM    1) pair discovery  - 1-month backtest per strategy over the full
REM       configHyper pair whitelist, to find which pairs it actually trades
REM    2) deep backtest   - ~1.5y backtest per strategy restricted to just
REM       the pairs it discovered
REM  Then writes a metrics_summary.txt (FULL METRICS SUMMARY per strategy)
REM  and generates user_data\configs\configBot<Strategy>.json (configHyper
REM  clone with pair_whitelist narrowed to that strategy's pairs).
REM
REM  Env overrides: CONFIG, JOBS, SCAN_DAYS, DEEP_DAYS.
REM
REM  Usage:
REM    run-backtest-suite.bat
REM    set JOBS=6&& run-backtest-suite.bat
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv\Scripts\activate.bat" (
    echo [run-backtest-suite] .venv not found - run install.bat first.
    exit /b 1
)
call ".venv\Scripts\activate.bat"

set "CONFIG=%CONFIG%"
if "%CONFIG%"=="" set "CONFIG=configHyper"

set "JOBS=%JOBS%"
if "%JOBS%"=="" set "JOBS=4"

set "SCAN_DAYS=%SCAN_DAYS%"
if "%SCAN_DAYS%"=="" set "SCAN_DAYS=30"

set "DEEP_DAYS=%DEEP_DAYS%"
if "%DEEP_DAYS%"=="" set "DEEP_DAYS=548"

echo [run-backtest-suite] config=%CONFIG% jobs=%JOBS% scan_days=%SCAN_DAYS% deep_days=%DEEP_DAYS%

python -m VulcanTrader.util.run_backtest_suite ^
    --config "%CONFIG%" ^
    --jobs %JOBS% ^
    --scan-days %SCAN_DAYS% ^
    --deep-days %DEEP_DAYS% %*

endlocal
