@echo off
echo 🚀 Starting DataHub Metadata Manager...
echo.

REM Check if virtual environment exists
if not exist "venv" (
    echo ❌ Virtual environment not found!
    echo Please run setup.py first:
    echo    python setup.py
    pause
    exit /b 1
)

REM Activate virtual environment and start application
call venv\Scripts\activate
echo ✅ Virtual environment activated
echo.

echo 🌐 Starting web server...
echo 📍 Application will be available at: http://localhost:5000
echo 🛑 Press Ctrl+C to stop the server
echo.

python run.py

pause