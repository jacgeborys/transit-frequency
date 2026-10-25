@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
set D=..\_data\warsaw\2026_10_04
echo %date% %time% FINALS_MB warsaw start (10 min 300 dpi, 5 min mb 04/05, 5 min 300 dpi) >> ..\png\log_buildings_final.txt
if exist ..\png\how_many_rides_in_10_mins_2026_10_07_dark_bmy_bright_hq.png goto c5
python render_poster.py --city warsaw --date 07.10.2026 --dpi 300 --data %D% --coverage %D%\coverage_map_gates_mb_10min.gpkg --buildings %D%\buildings_gates_mb_10min.gpkg --headline "Ile odjazdów komunikacji masz w zasięgu 10 minut pieszo?" --out ..\png\how_many_rides_in_10_mins_2026_10_07_dark_bmy_bright_hq.png > %D%\logs\render_10min_300.txt 2>&1
if errorlevel 1 (echo %date% %time% FINALS_MB render 10 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FINALS_MB render 10 min 300 done >> ..\png\log_buildings_final.txt
:c5
if exist %D%\coverage_map_gates_mb.gpkg goto c5r
python 04_coverage_raster.py --city warsaw --variant gates_mb --out %D%\coverage_map_gates_mb.gpkg %D% > %D%\logs\04_gates_mb.txt 2>&1
if errorlevel 1 (echo %date% %time% FINALS_MB 04 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
:c5r
if exist %D%\coverage_map_gates_mb_residents.gpkg goto b5
python 04_coverage_raster.py --city warsaw --variant gates_residents_mb --out %D%\coverage_map_gates_mb_residents.gpkg %D% > %D%\logs\04_gates_mb_residents.txt 2>&1
if errorlevel 1 (echo %date% %time% FINALS_MB 04 5 min residents FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
:b5
if exist %D%\buildings_gates_mb.gpkg goto r5
python 05_building_values.py --city warsaw --variant gates_mb %D% > %D%\logs\05_gates_mb.txt 2>&1
if errorlevel 1 (echo %date% %time% FINALS_MB 05 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FINALS_MB 5 min mb data done >> ..\png\log_buildings_final.txt
:r5
python render_poster.py --city warsaw --date 07.10.2026 --dpi 300 --data %D% --coverage %D%\coverage_map_gates_mb.gpkg --buildings %D%\buildings_gates_mb.gpkg --out ..\png\previews\warsaw_5min_mb_300.png > %D%\logs\render_5min_mb_300.txt 2>&1
if errorlevel 1 (echo %date% %time% FINALS_MB render 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FINALS_MB warsaw done >> ..\png\log_buildings_final.txt
