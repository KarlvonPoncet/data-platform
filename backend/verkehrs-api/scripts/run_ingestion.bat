@echo off
cd /d "%~dp0"
call ..\venv\Scripts\activate
python ingestion_service.py
pause
