@echo off
REM ---------------------------------------------------------------------------
REM  VulcanTrader - one-shot installer (Windows)
REM
REM  Creates a Python 3.12 virtualenv in .venv, upgrades pip, and installs
REM  every dependency listed in requirements.txt.
REM ---------------------------------------------------------------------------
setlocal enableextensions
cd /d "%~dp0"

if not exist ".venv" (
    echo [install] creating virtualenv .venv with Python 3.12 ...
    python -m venv .venv
    if errorlevel 1 (
        echo [install] ERROR: "py -3.12" failed. Install Python 3.12 from python.org first.
        exit /b 1
    )
) else (
    echo [install] reusing existing .venv
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 (
    echo [install] ERROR: failed to activate .venv
    exit /b 1
)

python -m pip install --upgrade pip wheel
if errorlevel 1 exit /b 1

python -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

echo.
echo [install] building the Rust backtester + vulcan_rust_indicators bridge (VulcanTrader\backtester) ...
echo [install]   One crate: the backtest engine, and the PyO3 indicator module that
echo [install]   AllIndicatorsDemoStrategy.py and several user_data\strategies\*.py import.
where cargo >nul 2>nul
if errorlevel 1 (
    echo [install] ERROR: "cargo" not found. Install the Rust toolchain from https://rustup.rs first.
    exit /b 1
)
REM maturin builds the vulcan_rust_indicators extension (which also compiles the
REM engine). PYO3_USE_ABI3_FORWARD_COMPATIBILITY allows building the abi3 module
REM against a Python newer than PyO3 explicitly supports.
pushd "VulcanTrader\backtester"
set PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
maturin develop --release --features extension-module
if errorlevel 1 (
    popd
    echo [install] ERROR: vulcan_rust_indicators build failed.
    exit /b 1
)
popd

echo.
echo [install] done. Activate later with:  .\.venv\Scripts\Activate.ps1
echo [install] then run:                   run-paper.bat   (or run-live.bat)
endlocal
