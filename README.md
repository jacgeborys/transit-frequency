# Transit Frequency Map

Visualizes transit accessibility by combining GTFS schedule data with walking isochrones.
Supports multiple cities — all scripts accept `--city <name>`.

## Supported cities

| City | GTFS source | Notes |
|------|------------|-------|
| **Warsaw** (default) | ZTM + KM + WKD via mkuran.pl | Multi-feed merge |
| **Poznan** | ZTM Poznan via mkuran.pl | Single feed |
| **Krakow** | ZTP Kraków bus + tram + regional trains | Multi-feed merge |
| **Gdansk** | ZTM Gdansk | City proper (bbox excludes Gdynia/Sopot) |
| **Berlin** | VBB GTFS | U/S-Bahn, trams, buses |

## Status (2026-10-09)

**Current product: dark posters**, 300 dpi, one per city, all for Wednesday 07.10.2026:

| City | File | Data folder |
|---|---|---|
| Warsaw | `png/how_many_rides_in_5_mins_2026_10_07_dark_bmy_bright_hq.png` | `_data/warsaw/2026_10_04` |
| Kraków | `png/how_many_rides_in_5_mins_krakow_2026_10_07_dark_bmy_bright_hq.png` | `_data/krakow/2026_10_04` |
| Berlin | `png/how_many_rides_in_5_mins_berlin_2026_10_07_dark_bmy_bright_hq.png` | `_data/berlin/2026_10_07` |

Render one with `python script/render_poster.py --city <city>` (style + header texts live
there). Pipeline variant: `--gates` isochrones + `05_building_values.py` (buildings coloured
by their best frequency, buildings behind fences painted from the residents-only coverage).
Style: near-black page/land, palette `bmy_dark` with a perceived-lightness floor
(`--min-lightness 34`, CIE L*: every class lighter than unserved buildings) and a brighter
top (`--palette-extend "#fff38a,#fffbd6"`), coverage fill 0.42, Bahnschrift header with a
big city name + question + date line, km scale bar opposite the legend.
Thumbnails of every map: `png/previews/*_thumb.png`.

Also available: light chroma maps `…_gates_buildings_chroma.png` (all three cities), other
dark palettes for Warsaw `…_dark_<palette>.png`, style comparison sheets
`png/previews/style_grid_*.png` (`script/_style_grid.py`), header font samples
(`script/_font_samples.py`), diagnostic access sketches `png/previews/sketch_*.png`
(`script/sketch_access.py`).

### Next steps / open ideas

- **Berlin legend is too small**: Berlin borrows Kraków's layout (page twice as wide), the
  header scales with page width but the legend does not. Make `draw_legend` scale with
  the page (like `draw_headline`) and re-render Berlin.
- **New cities** (e.g. Manhattan): see "Adding a new city" below.
- Readability ideas not done: fewer, wider classes (8-10 instead of 19); faint district
  labels; a legend line explaining residents-only areas.
- Heavy rail as a barrier (like fences, with openings at level crossings; skip
  bridge/tunnel segments) was discussed, not implemented.

### History

- 2026-10-04…06: Warsaw + Kraków recomputed for 07.10.2026 (school-term weekday; Warsaw
  886k departures vs 771k in the August summer timetable). Warsaw walking network rebuilt
  with `02_fetch_walking_network.py` (1.23 M nodes, incl. `access=private` paths); old
  OSMnx network kept as `network/warsaw/walking_network_osmnx_2026_02.*.bak`.
  Variants: baseline (50 m buffer), barriers, gates (Kraków covered area: 16,620 → 15,400
  → 14,015 ha public).
- 2026-10-07: buildings coloured by best frequency; standalone renderer replaces QGIS export.
- 2026-10-08: access-model fixes (fence leak via path cells, gate openings in the fence
  raster, `foot=yes` alone no longer opens a gate, public path islands behind closed gates
  count as private, restrictive `access` beats `opening_hours`, roofs/carports not drawn
  as buildings, coverage noding loss). Berlin added. Incremental coverage cache.
  Light chroma maps; dark mode explored.
- 2026-10-09: dark bmy_bright posters for all three cities; National Stadium fix (big
  buildings show residents-only coverage).

## Architecture

```
00_download_gtfs.py --city <name>       -- Download GTFS (auto-merge for Warsaw/Kraków)
    v
_data/<city>/YYYY_MM_DD/                -- Merged GTFS directory
    v
01_calculate_trip_counts.py --city X YYYYMMDD <folder>  -- Trips & routes per stop (06-22)
    v
02_fetch_walking_network.py --city      -- Tiled Overpass download (cached, retried, refuses
                                           to build with missing tiles) -> walking_network.pkl
    v
03_generate_isochrones_local.py         -- 5-min walking isochrones, parallel (--workers),
      [--barriers] [--gates]               checkpointed every 250 stops
    v
04_create_coverage_map.py [--variant V] -- Deduplicated frequency per area: tiled planar
                                           subdivision (STRtree, parallel, RAM guard) +
                                           parallel dissolve. Incremental: tiles on a fixed
                                           grid cached by a fingerprint of their isochrones
                                           in <data>/.coverage_cache_<V>/, so a rerun after a
                                           small rule change only redoes changed tiles
                                           (Kraków: 47 s all-cached vs ~7 min cold)
    v
05_building_values.py --variant V       -- (optional) best frequency per building
    v
render_qgis_style.py                    -- Standalone re-implementation of the QGIS layouts
      [--residents] [--buildings] [--palette] [--crop lon,lat,w,h] [--extend-left/right-m]
      (layer cache in cache/render/, rasterio drawing; a city renders in ~1-7 min;
      PNG written to system temp then copied into png/ - OneDrive locks fresh files)
render_poster.py --city C [--dpi]      -- Current product: dark bmy_bright poster (wraps the above)
sketch_access.py --city C --stop REGEX  -- Diagnostic access sketch around a stop
      [--recompute] [--name] [--suffix]    (--recompute: current rules, just these stops)
export_layout.py (QGIS python)          -- Exact export of the QGIS layout, project untouched
compare_barriers.py                     -- buffer vs barrier stats + close-ups

access_rules.py                         -- Gate / private-way rules for --gates
D:\QGIS\osm_basemap\fetch_osm_basemap.py -- Basemap fetcher (incl. barriers, gates, private_ways)
```

## Isochrone variants

- **baseline**: reachable network (375 m at 4.5 km/h) buffered by 50 m.
- **`--barriers`**: same routing; the 50 m off-network spread is a 2 m raster cost-distance
  where fences/walls/hedges are impassable and buildings are one-way space: entered like
  open ground (same budget) but never exited, so they are never a shortcut.
  Small holes (< 200 m²) filled, fragments (< 50 m²) dropped.
- **`--gates`** (implies barriers): two-layer routing. Closed gates and private ways form a
  residents-only layer you can enter from public paths but never leave back into public
  space (no shortcuts through estates). Outputs `isochrones_gates` (public) and
  `isochrones_gates_residents` (public + restricted). Rules in `access_rules.py`:
  explicit tags win, except `foot=yes` alone (often just means "pedestrian gate", e.g. on
  private campuses); untagged gates are closed except within 15 m of parks/cemeteries;
  lift gates ignored; ROD allotments count as closed. Gate nodes also cut a ~5 m opening
  in the fence raster (open gates: everyone; closed gates: residents run only), so gates
  without a mapped path through them still connect fenced plots. Path cells never
  override fence or closed-gate cells (sidewalks along fences would leak into plots).
  Public path "islands" (untagged paths reachable only through closed gates or private
  ways, e.g. stadium grounds, gated estates; < 5,000 nodes, not the main network) are
  reclassified private, so residents can walk them and stops never snap onto them.

## Adding a new city

1. **Config** in `script/cities.py`: `name`, `bbox`, `crs_metric` (a *metre*-based CRS: UTM
   zone or national grid), `gtfs` feed URL(s) + `gtfs_merge` ('single' or a merge mode),
   `vehicle_classify` (route -> bus/tram/train/metro), `has_frequencies` (feed uses
   frequencies.txt), `network_tiles` (Overpass tile grid; denser city -> more tiles),
   `template: 'krakow'` (no QGIS layout of its own: borrow Kraków's layout + styles).
2. **Poster texts**: add an entry to `POSTER` in `script/render_poster.py` (city name,
   language; English template `EN` is there).
3. **Run** (from `script/`; heavy steps one at a time, detached, see below):

```bash
python 00_download_gtfs.py --city X
python 01_calculate_trip_counts.py --city X YYYYMMDD ../_data/X/<folder>   # a school-term Wednesday
python D:/QGIS/osm_basemap/fetch_osm_basemap.py --city X                    # basemap incl. barriers, gates, private_ways, buildings
python 02_fetch_walking_network.py --city X
python 03_generate_isochrones_local.py --city X --gates ../_data/X/<folder>
python 04_create_coverage_map.py --city X --variant gates ../_data/X/<folder>
python 04_create_coverage_map.py --city X --variant gates_residents ../_data/X/<folder>
python 05_building_values.py --city X --variant gates ../_data/X/<folder>
python render_poster.py --city X --date DD.MM.YYYY
```

4. **Check** a few busy stops with `sketch_access.py --city X --stop "^Name$"` before
   trusting the map (access tagging differs between countries).

**Notes for Manhattan / New York** (not started):
- GTFS: MTA publishes separate feeds - subway, and buses per borough (Manhattan, Bronx,
  Brooklyn, Queens, Staten Island) plus MTA Bus Company; also PATH, LIRR, Metro-North,
  NYC Ferry. Needs a multi-feed merge like Warsaw (check `route_id`/`stop_id` collisions
  between feeds; prefix ids per feed). Some feeds have high `frequencies.txt`-style
  service; verify trip counts per stop look sane (Times Sq should be very high).
- Bbox: Manhattan alone is narrow; include a margin (Jersey City, Long Island City,
  south Bronx) so edge stops' isochrones aren't cut, then crop the render if wanted.
- CRS: use UTM 18N `EPSG:32618` (metres). Avoid NY State Plane `EPSG:2263` (US feet) -
  the pipeline assumes metres everywhere.
- OSM access tags: US gates/fences are tagged differently; expect many untagged gates in
  parks (rule: untagged gates near parks are open) and `access=private` on building
  courtyards. Check sketches around Central Park, Stuyvesant Town, Battery Park City.
- Basemap: very dense buildings; `fetch_osm_basemap.py` tiles buildings, should be fine.
  Water is large (rivers/harbour): the dark water colour matters for the look.
- Poster header in English (`EN` in `render_poster.py`), US-style date may be preferred.
- No QGIS layout: `template: 'krakow'` gives a landscape page; Manhattan is tall and
  narrow, so a portrait template (`'warsaw'`) is probably the better fit.

## Running on this machine (15 GB RAM)

Run heavy steps one at a time and detached from the terminal session (e.g. a `.cmd`
started with `Start-Process`), since 03/04/renders can each take 2–6 GB. All long steps
checkpoint (03 chunks, 04 tiles, basemap/network tiles), so an interrupted run resumes.

## Directory structure

```
_data/<city>/YYYY_MM_DD/   -- GTFS data + outputs (isochrones*, coverage_map*, buildings*, logs)
network/<city>/            -- Walking network (pickle) + tile cache
D:\QGIS\osm_basemap\<city>\  -- OSM basemap layers (shared across projects)
styles/                    -- QGIS QML styles
png/                       -- Rendered maps
```

## Key concepts

- **trip_count**: Total departures at a stop (6 AM - 10 PM)
- **unique_routes**: Number of distinct transit lines serving a stop
- **route_ids / route_trip_counts**: per-stop route lists enabling deduplication when
  isochrones from nearby stops overlap (per route, the best stop counts once)
- **Vehicle types**: bus, tram, train, metro — classified by city-specific route_id rules

## Data sources

- **GTFS**: [mkuran.pl](https://mkuran.pl/gtfs/) (Polish cities), ZTP Kraków, VBB (Berlin)
- **Walking network, barriers, gates, basemap**: OpenStreetMap via Overpass API
