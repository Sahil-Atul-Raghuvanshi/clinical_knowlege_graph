@echo off
echo Starting Clinical Knowledge Graph API on port 8002...
cd /d "%~dp0"
python -m uvicorn main:app --host 127.0.0.1 --port 8002 --reload
