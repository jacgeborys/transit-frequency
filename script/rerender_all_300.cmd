@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\script
echo %date% %time% RERENDER_ALL start (11 finals, 300 dpi) >> ..\png\log_buildings_final.txt
python render_poster.py --city tricity --date 14.10.2026 --data ..\_data\tricity\2026_10_12 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL tricity FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL tricity done >> ..\png\log_buildings_final.txt)
python render_poster.py --city zurich --date 14.10.2026 --data ..\_data\zurich\2026_10_11 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL zurich FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL zurich done >> ..\png\log_buildings_final.txt)
python render_poster.py --city amsterdam --date 14.10.2026 --data ..\_data\amsterdam\2026_10_10 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL amsterdam FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL amsterdam done >> ..\png\log_buildings_final.txt)
python render_poster.py --city lublin --date 14.10.2026 --data ..\_data\lublin\2026_10_09 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL lublin FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL lublin done >> ..\png\log_buildings_final.txt)
python render_poster.py --city krakow --date 07.10.2026 --data ..\_data\krakow\2026_10_04 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL krakow FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL krakow done >> ..\png\log_buildings_final.txt)
python render_poster.py --city boston --date 14.10.2026 --data ..\_data\boston\2026_10_13 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL boston FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL boston done >> ..\png\log_buildings_final.txt)
python render_poster.py --city paris --date 14.10.2026 --data ..\_data\paris\2026_10_12 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL paris FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL paris done >> ..\png\log_buildings_final.txt)
python render_poster.py --city sanfrancisco --date 14.10.2026 --data ..\_data\sanfrancisco\2026_10_11 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL sanfrancisco FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL sanfrancisco done >> ..\png\log_buildings_final.txt)
python render_poster.py --city warsaw --date 07.10.2026 --data ..\_data\warsaw\2026_10_04 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL warsaw FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL warsaw done >> ..\png\log_buildings_final.txt)
python render_poster.py --city newyork --date 14.10.2026 --data ..\_data\newyork\2026_10_11 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL newyork FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL newyork done >> ..\png\log_buildings_final.txt)
python render_poster.py --city berlin --date 07.10.2026 --data ..\_data\berlin\2026_10_07 --dpi 300 >> ..\png\rerender_all_300.txt 2>&1
if errorlevel 1 (echo %date% %time% RERENDER_ALL berlin FAILED >> ..\png\log_buildings_final.txt) else (echo %date% %time% RERENDER_ALL berlin done >> ..\png\log_buildings_final.txt)
echo %date% %time% RERENDER_ALL ALL DONE >> ..\png\log_buildings_final.txt
