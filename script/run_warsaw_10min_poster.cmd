@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
set D=..\_data\warsaw\2026_10_04
start "" /b python _ram_guard.py --interval 15 --match 03_generate_isochrones_local 06_building_modes 04_coverage_raster 05_building_values render_qgis_style > %D%\logs\ram_guard_all.txt 2>&1
echo %date% %time% POSTER10 warsaw start (04 raster x2, 05, render 150 dpi) >> ..\png\log_buildings_final.txt
if exist %D%\coverage_map_gates_mb_10min.gpkg goto res
python 04_coverage_raster.py --city warsaw --variant gates_mb_10min --out %D%\coverage_map_gates_mb_10min.gpkg %D% > %D%\logs\04_gates_mb_10min.txt 2>&1
if errorlevel 1 (echo %date% %time% POSTER10 04 public FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% POSTER10 04 public done >> ..\png\log_buildings_final.txt
:res
if exist %D%\coverage_map_gates_mb_10min_residents.gpkg goto b05
python 04_coverage_raster.py --city warsaw --variant gates_residents_mb_10min --out %D%\coverage_map_gates_mb_10min_residents.gpkg %D% > %D%\logs\04_gates_mb_10min_residents.txt 2>&1
if errorlevel 1 (echo %date% %time% POSTER10 04 residents FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% POSTER10 04 residents done >> ..\png\log_buildings_final.txt
:b05
if exist %D%\buildings_gates_mb_10min.gpkg goto render
python 05_building_values.py --city warsaw --variant gates_mb_10min %D% > %D%\logs\05_gates_mb_10min.txt 2>&1
if errorlevel 1 (echo %date% %time% POSTER10 05 FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% POSTER10 05 done >> ..\png\log_buildings_final.txt
:render
echo %date% %time% POSTER10 ready for render >> ..\png\log_buildings_final.txt
