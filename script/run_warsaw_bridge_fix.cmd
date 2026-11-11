@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
set D=..\_data\warsaw\2026_10_04
set B=%D%\before_bridge_fix
if not exist %D%\logs mkdir %D%\logs
if not exist %B% mkdir %B%
start "" /b python _ram_guard.py --match 03_generate_isochrones_local 04_coverage_raster 05_building_values > %D%\logs\ram_guard_bridge_fix.txt 2>&1
echo %date% %time% BRIDGE_FIX warsaw start (old 5-min mb files to before_bridge_fix, 03 mb, 04 x2, 05) >> ..\png\log_buildings_final.txt
for %%F in (isochrones_gates_mb.gpkg isochrones_gates_residents_mb.gpkg coverage_map_gates_mb.gpkg coverage_map_gates_mb_residents.gpkg buildings_gates_mb.gpkg) do if exist %D%\%%F if not exist %B%\%%F move %D%\%%F %B%\ >nul
if exist %D%\isochrones_gates_residents_mb.gpkg goto c5
python 03_generate_isochrones_local.py --city warsaw --major-barriers --workers 6 %D% > %D%\logs\03_gates_mb2.txt 2>&1
if errorlevel 1 (echo %date% %time% BRIDGE_FIX 03 FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% BRIDGE_FIX 03 done >> ..\png\log_buildings_final.txt
:c5
if exist %D%\coverage_map_gates_mb.gpkg goto c5r
python 04_coverage_raster.py --city warsaw --variant gates_mb --out %D%\coverage_map_gates_mb.gpkg %D% > %D%\logs\04_gates_mb2.txt 2>&1
if errorlevel 1 (echo %date% %time% BRIDGE_FIX 04 FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% BRIDGE_FIX 04 public done >> ..\png\log_buildings_final.txt
:c5r
if exist %D%\coverage_map_gates_mb_residents.gpkg goto b5
python 04_coverage_raster.py --city warsaw --variant gates_residents_mb --out %D%\coverage_map_gates_mb_residents.gpkg %D% > %D%\logs\04_gates_mb2_residents.txt 2>&1
if errorlevel 1 (echo %date% %time% BRIDGE_FIX 04 residents FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% BRIDGE_FIX 04 residents done >> ..\png\log_buildings_final.txt
:b5
if exist %D%\buildings_gates_mb.gpkg goto done
python 05_building_values.py --city warsaw --variant gates_mb %D% > %D%\logs\05_gates_mb2.txt 2>&1
if errorlevel 1 (echo %date% %time% BRIDGE_FIX 05 FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
:done
echo %date% %time% BRIDGE_FIX warsaw done >> ..\png\log_buildings_final.txt
