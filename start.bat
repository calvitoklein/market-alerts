@echo off
title Signal Monitor - Lanzador
color 0A
cd /d %~dp0
set PYTHONIOENCODING=utf-8

echo ==========================================
echo   SIGNAL MONITOR BOT  v2
echo   XAU / BTC / ETH / SOL + Insiders
echo ==========================================
echo.

echo Arrancando bot principal...
start "BOT - tweet_monitor" cmd /k "set PYTHONIOENCODING=utf-8 && python -u tweet_monitor.py --loop"

echo Arrancando dashboard (localhost:5000)...
start "DASHBOARD - flask" cmd /k "set PYTHONIOENCODING=utf-8 && python dashboard.py"

timeout /t 3 /nobreak >nul
echo Abriendo dashboard en el navegador...
start http://localhost:5000

echo.
echo Todo lanzado. Cierra las ventanas del bot y dashboard para parar.
pause
