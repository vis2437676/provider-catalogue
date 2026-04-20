@echo off
cd /d "%~dp0"

echo Installing dependencies...
pip install -q -r requirements.txt

echo.
echo Starting Lab Test Mapping Console on http://localhost:8010
echo.
start "" http://localhost:8010
uvicorn server:app --host 127.0.0.1 --port 8010 --reload
