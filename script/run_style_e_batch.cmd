@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
echo %date% %time% STYLE_E_BATCH start (05 tower fix + 300 dpi style E: newyork, boston, paris, sanfrancisco, zurich, amsterdam, tricity) >> ..\png\log_buildings_final.txt
if not exist ..\_data\newyork\2026_10_11\logs mkdir ..\_data\newyork\2026_10_11\logs
python _wait_ram.py 6
python 05_building_values.py --city newyork --variant gates ..\_data\newyork\2026_10_11 > ..\_data\newyork\2026_10_11\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH newyork 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city newyork --date 14.10.2026 --data ..\_data\newyork\2026_10_11 --dpi 300 > ..\_data\newyork\2026_10_11\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH newyork render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH newyork done >> ..\png\log_buildings_final.txt)
)
if not exist ..\_data\boston\2026_10_13\logs mkdir ..\_data\boston\2026_10_13\logs
python _wait_ram.py 6
python 05_building_values.py --city boston --variant gates ..\_data\boston\2026_10_13 > ..\_data\boston\2026_10_13\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH boston 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city boston --date 14.10.2026 --data ..\_data\boston\2026_10_13 --dpi 300 > ..\_data\boston\2026_10_13\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH boston render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH boston done >> ..\png\log_buildings_final.txt)
)
if not exist ..\_data\paris\2026_10_12\logs mkdir ..\_data\paris\2026_10_12\logs
python _wait_ram.py 6
python 05_building_values.py --city paris --variant gates ..\_data\paris\2026_10_12 > ..\_data\paris\2026_10_12\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH paris 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city paris --date 14.10.2026 --data ..\_data\paris\2026_10_12 --dpi 300 > ..\_data\paris\2026_10_12\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH paris render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH paris done >> ..\png\log_buildings_final.txt)
)
if not exist ..\_data\sanfrancisco\2026_10_11\logs mkdir ..\_data\sanfrancisco\2026_10_11\logs
python _wait_ram.py 6
python 05_building_values.py --city sanfrancisco --variant gates ..\_data\sanfrancisco\2026_10_11 > ..\_data\sanfrancisco\2026_10_11\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH sanfrancisco 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city sanfrancisco --date 14.10.2026 --data ..\_data\sanfrancisco\2026_10_11 --dpi 300 > ..\_data\sanfrancisco\2026_10_11\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH sanfrancisco render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH sanfrancisco done >> ..\png\log_buildings_final.txt)
)
if not exist ..\_data\zurich\2026_10_11\logs mkdir ..\_data\zurich\2026_10_11\logs
python _wait_ram.py 6
python 05_building_values.py --city zurich --variant gates ..\_data\zurich\2026_10_11 > ..\_data\zurich\2026_10_11\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH zurich 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city zurich --date 14.10.2026 --data ..\_data\zurich\2026_10_11 --dpi 300 > ..\_data\zurich\2026_10_11\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH zurich render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH zurich done >> ..\png\log_buildings_final.txt)
)
if not exist ..\_data\amsterdam\2026_10_10\logs mkdir ..\_data\amsterdam\2026_10_10\logs
python _wait_ram.py 6
python 05_building_values.py --city amsterdam --variant gates ..\_data\amsterdam\2026_10_10 > ..\_data\amsterdam\2026_10_10\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH amsterdam 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city amsterdam --date 14.10.2026 --data ..\_data\amsterdam\2026_10_10 --dpi 300 > ..\_data\amsterdam\2026_10_10\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH amsterdam render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH amsterdam done >> ..\png\log_buildings_final.txt)
)
if not exist ..\_data\tricity\2026_10_12\logs mkdir ..\_data\tricity\2026_10_12\logs
python _wait_ram.py 6
python 05_building_values.py --city tricity --variant gates ..\_data\tricity\2026_10_12 > ..\_data\tricity\2026_10_12\logs\05_fix_overlap.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH tricity 05 FAILED >> ..\png\log_buildings_final.txt) else (
python render_poster.py --city tricity --date 14.10.2026 --data ..\_data\tricity\2026_10_12 --dpi 300 > ..\_data\tricity\2026_10_12\logs\render_300_styleE.txt 2>&1
if errorlevel 1 (echo %date% %time% STYLE_E_BATCH tricity render FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% STYLE_E_BATCH tricity done >> ..\png\log_buildings_final.txt)
)
echo %date% %time% STYLE_E_BATCH all done >> ..\png\log_buildings_final.txt
