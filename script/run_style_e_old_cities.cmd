@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
start "" /b python _ram_guard.py --interval 15 --match 05_building_values render_qgis_style > ..\_data\krakow\2026_10_04\ram_guard_pre_e.txt 2>&1
echo %date% %time% STYLE_E_OLD start (05 tower fix + 300 dpi style E on the existing walk areas: lublin, krakow, berlin) >> ..\png\log_buildings_final.txt
python _wait_ram.py 6
python 05_building_values.py --city lublin --variant gates ..\_data\lublin\2026_10_09 > ..\_data\lublin\2026_10_09\log_05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_OLD lublin 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city lublin --date 14.10.2026 --data ..\_data\lublin\2026_10_09 --dpi 300 > ..\_data\lublin\2026_10_09\log_render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_OLD lublin render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_OLD lublin done >> ..\png\log_buildings_final.txt)
)
python _wait_ram.py 6
python 05_building_values.py --city krakow --variant gates ..\_data\krakow\2026_10_04 > ..\_data\krakow\2026_10_04\log_05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_OLD krakow 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city krakow --date 07.10.2026 --data ..\_data\krakow\2026_10_04 --dpi 300 > ..\_data\krakow\2026_10_04\log_render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_OLD krakow render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_OLD krakow done >> ..\png\log_buildings_final.txt)
)
python _wait_ram.py 6
python 05_building_values.py --city berlin --variant gates ..\_data\berlin\2026_10_07 > ..\_data\berlin\2026_10_07\log_05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_OLD berlin 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city berlin --date 07.10.2026 --data ..\_data\berlin\2026_10_07 --dpi 300 > ..\_data\berlin\2026_10_07\log_render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_OLD berlin render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_OLD berlin done >> ..\png\log_buildings_final.txt)
)
echo %date% %time% STYLE_E_OLD all done >> ..\png\log_buildings_final.txt
