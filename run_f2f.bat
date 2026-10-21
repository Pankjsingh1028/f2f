@echo off
title F2F
rem Project folder - change this one line if the project is moved
set "PROJECT_DIR=C:\Users\AMAN\Documents\GitHub\f2f"
cd /d "%PROJECT_DIR%"

rem Scripts print emoji; the default console code page cannot encode them
set PYTHONUTF8=1

rem Steps 1-3 must succeed, otherwise stop
for %%S in (autologin f1 fsl) do (
    echo.
    echo ===== Running %%S.py =====
    python %%S.py
    if errorlevel 1 (
        echo.
        echo [ERROR] %%S.py failed. Stopping.
        pause
        exit /b 1
    )
)

rem Steps 4-5: a failure is reported but the run carries on
for %%S in (build_futures_close build_future_spreads) do (
    echo.
    echo ===== Running %%S.py =====
    python %%S.py
    if errorlevel 1 echo [WARN] %%S.py failed. Continuing.
)

echo.
choice /c YN /m "Run f2fcalc.py"
if errorlevel 2 goto skip_calc

echo.
echo ===== Running f2fcalc.py =====
python f2fcalc.py
if errorlevel 1 (
    echo.
    echo [ERROR] f2fcalc.py failed. Stopping.
    pause
    exit /b 1
)
goto run_spread

:skip_calc
echo Skipping f2fcalc.py

:run_spread
echo.
echo ===== Running f2fspread.py =====
python f2fspread.py

pause
