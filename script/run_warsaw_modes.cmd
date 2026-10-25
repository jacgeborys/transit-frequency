@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
set D=..\_data\warsaw\2026_10_04
if not exist %D%\logs mkdir %D%\logs
start "" /b python _ram_guard.py --match 03_generate_isochrones_local 06_building_modes > %D%\logs\ram_guard.txt 2>&1
echo %date% %time% MODES warsaw start (03 mb 5 min, 03 mb 10 min, 06; steps with existing output skipped) >> ..\png\log_buildings_final.txt
if exist %D%\isochrones_gates_residents_mb.gpkg goto iso10
python 03_generate_isochrones_local.py --city warsaw --major-barriers %D% > %D%\logs\03_gates_mb.txt 2>&1
if errorlevel 1 (echo %date% %time% MODES 03 mb 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% MODES 03 mb 5 min done >> ..\png\log_buildings_final.txt
:iso10
if exist %D%\isochrones_gates_residents_mb_10min.gpkg goto modes
python 03_generate_isochrones_local.py --city warsaw --major-barriers --minutes 10 %D% > %D%\logs\03_gates_mb_10min.txt 2>&1
if errorlevel 1 (echo %date% %time% MODES 03 mb 10 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% MODES 03 mb 10 min done >> ..\png\log_buildings_final.txt
:modes
python 06_building_modes.py --city warsaw --date 20261007 --variant gates_mb --new-tram warsaw_tram_openings.csv %D% > %D%\logs\06_building_modes.txt 2>&1
if errorlevel 1 (echo %date% %time% MODES 06 FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% MODES warsaw done >> ..\png\log_buildings_final.txt
