@echo off
REM Rebuilds the vulcan_rust_indicators PyO3 extension and installs it over
REM the live Python environment's copy. Must run from a shell where MSVC's
REM link.exe resolves (Git Bash's PATH shadows it with coreutils' link) -
REM this script self-loads vcvars64.bat so it works from any shell.
REM
REM After ANY rebuild, re-verify against every vri.* name strategies call -
REM this module's #[pymodule] registration has silently dropped functions
REM between source revisions multiple times with no compile-time signal
REM (see project memory: rust-indicator-bridge).

call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"
cd /d "%~dp0"
cargo build --release --features extension-module
if errorlevel 1 exit /b 1

for /f "delims=" %%p in ('python -c "import site; print(site.getusersitepackages())"') do set SITE=%%p
copy /y "target\release\vulcan_rust_indicators.dll" "%SITE%\vulcan_rust_indicators.pyd"

python -c "import vulcan_rust_indicators as vri; needed=['adx','ema','atr','rsi','sma','bollinger_bands','calculate_standard_indicators','compute_result_metrics','fill_missing_candles','run_backtest']; missing=[n for n in needed if n not in dir(vri)]; print('MISSING:', missing) if missing else print('OK - all', len(needed), 'functions present')"
