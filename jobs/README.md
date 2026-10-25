# Jobs reachable by public transport (subproject)

Dark poster in the transit-frequency style: how many jobs you reach by public transport within 30 min, door to
door (walk + wait + ride + transfers), morning peak Wed 7.10.2026.

| Script | Python | Output |
|---|---|---|
| `travel_times_all.py` | `D:/QGIS/ceny_mieszkan/access/.venv` (r5py + portable Java 21) | `data/access_<scenario>_all.csv`: jobs_30 / jobs_45 / jobs_grav per 250 m cell, for every cell holding a building of the basemap (10,097 cells, ~2 h) |
| `make_jobs_poster.py` | main | `data/buildings_<measure>_<scenario>.gpkg` (value in thousands as `max_trips`, so `render_qgis_style.py` colours it), `renders/warsaw_<measure>_<scenario>_<dpi>.png` |
| `run_jobs_all.cmd` | | hidden chain: routing, then 150 dpi preview |

Routing, destinations and settings come from `ceny_mieszkan/access` (read only): R5 on the OSM walking network +
the Warsaw GTFS (ZTM, KM, WKD), median travel time over every departure minute 07-09, jobs = BDOT10k floor area
per job by building category (`ceny_mieszkan/access/build_inputs.py`). The ceny_mieszkan run routed only the 3,255
cells with priced flats; the first preview (`renders/warsaw_jobs_30_am_peak_150.png`, 2026-10-25) used those,
borrowing the nearest routed cell within 375 m, so many buildings stayed grey.

Poster: same `STYLE` as `script/render_poster.py`, 25 classes 1..650+ thousand jobs, no walk-area fill (250 m
cells would show as blocks).

## Status (2026-10-25)
Full run done: `travel_times_all.py --scenario am_peak` routed all 10,097 cells in 80 min (R5 network cached in
%LOCALAPPDATA%/r5py from the ceny_mieszkan run; ~0.5 s per cell). Median cell: 6,959 jobs within 30 min (the
priced-only cells: 56k; the new cells are mostly suburban). Poster: 253,313 buildings coloured from their own cell;
the rest reach no jobs within 30 min. 150 dpi preview `renders/warsaw_jobs_30_am_peak_150.png` (4.5 min).
Next if wanted: 300 dpi final (`--dpi 300`), other windows (`--scenario midday|evening|saturday`, each needs its
own routing run first), `--measure jobs_45`, a 125 m grid (~32k cells, ~4 h).
