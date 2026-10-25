@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
set D=..\_data\warsaw\2026_10_04
echo %date% %time% FIX05 warsaw start (05 overlap fix: gates_mb, gates_mb_10min; 3 finals) >> ..\png\log_buildings_final.txt
python 05_building_values.py --city warsaw --variant gates_mb %D% > %D%\logs\05_gates_mb_fix.txt 2>&1
if errorlevel 1 (echo %date% %time% FIX05 05 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
python 05_building_values.py --city warsaw --variant gates_mb_10min %D% > %D%\logs\05_gates_mb_10min_fix.txt 2>&1
if errorlevel 1 (echo %date% %time% FIX05 05 10 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FIX05 05 done >> ..\png\log_buildings_final.txt
python render_poster.py --city warsaw --date 07.10.2026 --dpi 300 --data %D% --coverage %D%\coverage_map_gates_mb.gpkg --buildings %D%\buildings_gates_mb.gpkg > %D%\logs\render_5min_fix.txt 2>&1
if errorlevel 1 (echo %date% %time% FIX05 render 5 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FIX05 render 5 min done >> ..\png\log_buildings_final.txt
python render_poster.py --city warsaw --date 07.10.2026 --dpi 300 --data %D% --coverage %D%\coverage_map_gates_mb_10min.gpkg --buildings %D%\buildings_gates_mb_10min.gpkg --headline "Ile odjazdów komunikacji masz w zasięgu 10 minut pieszo?" --out ..\png\how_many_rides_in_10_mins_2026_10_07_dark_bmy_bright_hq.png > %D%\logs\render_10min_fix.txt 2>&1
if errorlevel 1 (echo %date% %time% FIX05 render 10 min FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FIX05 render 10 min done >> ..\png\log_buildings_final.txt
python render_poster.py --city warsaw --date 07.10.2026 --dpi 300 --data %D% --coverage %D%\coverage_map_gates_mb_10min.gpkg --buildings %D%\buildings_gates_mb_10min.gpkg --headline "Ile odjazdów komunikacji masz w zasięgu 10 minut pieszo?" --breaks 1,5,10,50,75,100,150,175,250,300,400,500,700,1000,1200,1400,1500,1750,2000,2500,2900,3200,4000,4500,5000 --legend-ticks 1,10,100,250,500,1000,2000,5000 --out ..\png\how_many_rides_in_10_mins_2026_10_07_dark_bmy_bright_hq_rescaled.png > %D%\logs\render_10min_rescaled_fix.txt 2>&1
if errorlevel 1 (echo %date% %time% FIX05 render 10 min rescaled FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% FIX05 warsaw done >> ..\png\log_buildings_final.txt
