@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
set D=..\_data\warsaw6_10_04
if not exist %D%\logs mkdir %D%\logs
echo %date% %time% MODES warsaw start (03 mb 5 min, 03 mb 10 min, 06) >> ..\png\log_buildings_final.txt
python 03_generate_isochrones_local.py --city warsaw --major-barriers %D% > %D%\logs_gates_mb.txt 2>&1
if errorlevel 1 (echo %date% %time% MODES 03 mb 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% MODES 03 mb 5 min done >> ..\png\log_buildings_final.txt
python 03_generate_isochrones_local.py --city warsaw --major-barriers --minutes 10 %D% > %D%\logs_gates_mb_10min.txt 2>&1
if errorlevel 1 (echo %date% %time% MODES 03 mb 10 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% MODES 03 mb 10 min done >> ..\png\log_buildings_final.txt
python 06_building_modes.py --city warsaw --date 20261007 --variant gates_mb --new-tram warsaw_tram_openings.csv %D% > %D%\logs_building_modes.txt 2>&1
if errorlevel 1 (echo %date% %time% MODES 06 FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% MODES warsaw done >> ..\png\log_buildings_final.txt
