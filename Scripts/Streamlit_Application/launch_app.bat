@echo off
REM Streamlit App Launcher for Windows
REM Double-click this file to launch the Patient Analysis Dashboard

echo ============================================================
echo Launching Patient Analysis Dashboard...
echo ============================================================
echo.

REM Get the directory where this batch file is located
cd /d "%~dp0"

REM Check if app.py exists
if not exist "app.py" (
    echo Error: app.py not found in current directory
    echo Current directory: %CD%
    pause
    exit /b 1
)

echo App location: %CD%\app.py
echo.
echo The app will open in your default web browser.
echo To stop the app, close this window or press Ctrl+C
echo ============================================================
echo.

REM Launch Streamlit
python -m streamlit run app.py

REM If there's an error, pause so user can see it
if errorlevel 1 (
    echo.
    echo Error launching Streamlit!
    echo Make sure Streamlit is installed: pip install streamlit
    echo.
    pause
)

