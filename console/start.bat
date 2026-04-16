@echo off
cd /d "%~dp0"

echo Installing dependencies...
pip install -q -r requirements.txt

echo.
echo Starting Lab Test Mapping Console on http://localhost:8001
echo.
start "" http://localhost:8001
uvicorn server:app --host 0.0.0.0 --port 8001 --reload
