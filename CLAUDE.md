# TransitFrequency — notes for Claude

Pipeline, architecture and status live in `README.md`; keep it updated. This file covers
where data comes from and how to produce maps.

## Basemap / geodata sources (prefer in this order)

1. **`D:\QGIS\osm_basemap\<city>\`**: OSM layers fetched by `D:\QGIS\osm_basemap\fetch_osm_basemap.py`
   (not a git repo). Layers: water, waterways, parks, leisure, forests, grass, meadow,
   allotments, cemeteries, railways, roads, buildings, **barriers** (fences/walls/hedges,
   closed rings stored as polygons). Add a layer: `--city <c> --only <layer>`; existing
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

The PNGs in `png/` come from print layouts in `transit-frequency-map.qgz` (one per city;
the layout maps show whichever layer-tree group under `Tlo` is visible). Export headless,
leaving the project file unmodified:

```
"C:\Program Files\QGIS 3.28.3\bin\python-qgis.bat" script\export_layout.py --city warsaw ^
    --coverage _data\warsaw\<folder>\coverage_map.gpkg --date DD.MM.YYYY --out png\<name>.png
```

`script/render_map.py` is a separate dark-theme matplotlib render, not the published style.
