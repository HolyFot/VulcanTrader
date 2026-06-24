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
        echo [install] ERROR: "python" failed. Install Python 3.12 from python.org first.
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
echo [install] done. Activate later with:  .\.venv\Scripts\Activate.ps1
echo [install] then run:                   run-paper.bat   (or run-live.bat)
endlocal
