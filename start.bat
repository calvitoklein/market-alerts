@echo off
title Market Alert Monitor - NQ / SP500 / BTC
color 0A
cd /d %~dp0

for /f "tokens=1,2 delims==" %%a in (.env) do set %%a=%%b
set PYTHONIOENCODING=utf-8

echo ==========================================
echo   MARKET ALERT MONITOR  (NQ / SP500 / BTC)
echo   Revisa cada 2 min - Cierra para parar
echo ==========================================
echo.

python tweet_monitor.py --loop

pause
