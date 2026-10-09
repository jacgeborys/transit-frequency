# TransitFrequency — notes for Claude

Pipeline, architecture and status live in `README.md`; keep it updated. This file covers
where data comes from and how to produce maps.

## Basemap / geodata sources (prefer in this order)

1. **`D:\QGIS\osm_basemap\<city>\`**: OSM layers fetched by `D:\QGIS\osm_basemap\fetch_osm_basemap.py`
   (not a git repo). Layers: water, waterways, parks, leisure, forests, grass, meadow,
   allotments, cemeteries, railways, roads, buildings, **barriers** (fences/walls/hedges,
   closed rings stored as polygons), **sea** (built from `natural=coastline` and appended to
   water.gpkg: tidal straits like New York's Harlem River are not mapped as water areas).
   A layer that still fails after retry rounds makes the fetcher exit 1 (no silent "No data").
   Both fetchers send Overpass requests through `overpass_polite.py` (waits for a free slot via
   /api/status, backs off after 504s, drops a mirror after 3 failures). The copy next to the
   basemap fetcher on D: must be kept in sync with `script/overpass_polite.py`. Add a layer: `--city <c> --only <layer>`; existing
   .gpkg files are skipped, so delete one to refetch it.
2. **`D:\QGIS\bdot_basemap\<Area>\`**: BDOT10k shapefiles (GUGiK), currently Warszawa only
   (`PL.PZGiK.330.1465__OT_*.shp`, e.g. `OT_BUBD_A` = buildings). Other areas must be
   downloaded per powiat from geoportal/GUGiK. Use it only when OSM is lacking.
3. `D:\QGIS\mapy_warszawy_misc\data\osm\`: older Warsaw OSM extract; the Warsaw layout
   in the QGIS project still uses it for the background.

## Choosing a GTFS date

- Pick a regular school-term weekday, usually a **Wednesday in October** (or March to May).
  Avoid July and August: the summer timetable reduces Warsaw departures by ~15%.
- `00_download_gtfs.py`'s "top dates" list is skewed by train-feed history. Check trip
  counts for the specific dates you're considering.
- Pass the date explicitly: `01_calculate_trip_counts.py --city X YYYYMMDD <data_folder>`.

## Barrier-aware isochrones

`03_generate_isochrones_local.py --barriers` and `04_create_coverage_map.py --barriers` produce
`isochrones_barriers.gpkg` / `coverage_map_barriers.gpkg`. Network routing is unchanged; the
50 m off-network spread becomes a 2 m raster cost-distance where fences/walls are solid and
buildings can be entered 10 m deep but not crossed. Needs `buildings.gpkg` + `barriers.gpkg`
in the city's osm_basemap folder.

## Changing access rules: check on sketches, rerun in batches

Full city reruns are slow (coverage step: Warsaw ~2x20 min, Berlin ~2x30 min). When tuning
`access_rules.py` or the barrier model:
1. Check each change on a sketch: `script/sketch_access.py --city X --stop "^Name$"`
   (regex on stop names; `--name` for the title/file name, `--suffix` if the PNG is locked).
   Add `--recompute` to rebuild just those stops with the current rules (~1 min Kraków,
   a few min Warsaw/Berlin) instead of reading the city's (possibly stale) isochrones.
2. Collect several fixes, then rerun each city once (03 -> 04 gates -> 04 gates_residents
   -> 05 -> render). Bump the `_gN` checkpoint tag in 03 when access rules change.

## Rendering the published maps

**Current product = dark poster**: `python script/render_poster.py --city <city>` (300 dpi;
`--dpi 150` for working renders). It wraps `script/render_qgis_style.py`, a standalone
re-implementation of the QGIS print layouts in `transit-frequency-map.qgz` (it reads the
project for layout, styles and layer stack; cities without a layout use `template` from
cities.py). Header texts per city: `POSTER` in render_poster.py. Renders write via the
system temp folder (OneDrive locks fresh files in png/).

Style experiments: `script/_style_grid.py --set light|dark --crop lon,lat,w_m,h_m --name N`
renders one crop in many variants side by side (png/previews/style_grid_N.png). Crops are
small at layout scale: use `--dpi 200-500` for them.

Old route (exact QGIS export, project untouched):
`"C:\Program Files\QGIS 3.28.3in\python-qgis.bat" script\export_layout.py --city warsaw
--coverage ... --date DD.MM.YYYY --out png\<name>.png`. `script/render_map.py` is an old
unrelated matplotlib render.

## Running long jobs on this machine

- 15 GB RAM; the user often has PyCharm open (~4 GB). Check free RAM before heavy steps.
- Run multi-step chains as a `.cmd` started detached (`Start-Process cmd -ArgumentList
  '/c','"<file>.cmd"' -WindowStyle Hidden`), so they survive the session. **The .cmd must
  have CRLF line endings** (LF-only files silently never start) and, if it contains
  Polish/German characters, be UTF-8 with `chcp 65001 >nul` at the top. Verify it started
  (log line or `python.exe` process) right after launching.
- Log chain milestones to `png/log_buildings_final.txt` and watch them with a Monitor.
- Save every render/preview as PNG (previews in `png/previews/`); working renders 150 dpi,
  300 dpi only for requested finals.
