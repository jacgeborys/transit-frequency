@echo off
chcp 65001 >nul
cd /d D:\QGIS\TransitFrequency\jobs
if not exist logs mkdir logs
echo %date% %time% JOBS travel_times_all am_peak start >> ..\png\log_buildings_final.txt
D:\QGIS\ceny_mieszkan\access\.venv\Scripts\python.exe travel_times_all.py --scenario am_peak > logs\travel_times_all_am_peak.txt 2>&1
if errorlevel 1 (echo %date% %time% JOBS travel_times_all FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% JOBS travel_times_all done >> ..\png\log_buildings_final.txt
python make_jobs_poster.py --scenario am_peak --dpi 150 > logs\poster_am_peak_150.txt 2>&1
if errorlevel 1 (echo %date% %time% JOBS poster FAILED >> ..\png\log_buildings_final.txt & exit /b 1)
echo %date% %time% JOBS poster 150 done >> ..\png\log_buildings_final.txt
