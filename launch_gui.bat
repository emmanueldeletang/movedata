@echo off
REM Launch the Cosmos DB Migration Tool GUI
echo Starting Azure Cosmos DB MongoDB Migration Tool GUI...

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python 3.8 or later from https://python.org
    pause
    exit /b 1
)

REM Check if virtual environment exists
if exist "venv\Scripts\activate.bat" (
    echo Activating virtual environment...
    call venv\Scripts\activate.bat
) else (
    echo Virtual environment not found. Installing dependencies globally...
)

REM Install dependencies if needed
echo Installing/updating dependencies...
pip install -r requirements.txt

REM Launch the GUI
echo Launching GUI...
python gui_launcher.py

pause
