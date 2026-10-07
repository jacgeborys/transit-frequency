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

## Status

**October 2026 refresh (2026-10-04 … 10-06):** Warsaw and Kraków recomputed for
**Wednesday 07.10.2026** (school-term weekday). Warsaw: 886k departures vs 771k in the
August (summer-timetable) map. Kraków: 283k departures.

Map variants now produced (Warsaw + Kraków, `png/how_many_rides_in_5_mins[_krakow]_2026_10_07*.png`):

| Variant | Files | Method |
|---|---|---|
| baseline | `…_2026_10_07.png` | 50 m buffer around the reachable network (old method) |
| barriers | `…_barriers.png` | fences/walls block, buildings enterable 10 m (Kraków only; Warsaw barrier-only rerun skipped) |
| gates | `…_gates.png` | barriers + closed gates/private paths; restricted areas drawn pale |

Covered area, Kraków: baseline 16,620 ha → barriers 15,400 ha → gates public 14,015 ha
(+~950 ha restricted-only). Warsaw baseline: 28,413 ha.

**Warsaw walking network rebuilt 2026-10-06** with `02_fetch_walking_network.py`
(1.23 M nodes, 2.78 M edges, unsimplified, includes `access=private` paths). The old
February OSMnx network (simplified, no private paths) is kept as
`network/warsaw/walking_network_osmnx_2026_02.*.bak`. The Warsaw *baseline* map still
uses the old network.

**Current product (2026-10-07):** `png/how_many_rides_in_5_mins[_krakow]_2026_10_07_gates_buildings.png`
— gates variant, buildings coloured by their best frequency (`05_building_values.py`),
big buildings (> 5,000 m²) show the coverage inside their footprint, restricted areas off.
Style: QGIS palette, `--building-shade 0.80 --building-saturation 1.5 --coverage-fade 0.45
--uncovered-rgb 170,170,170`, 150 dpi; Warsaw `--extend-left-m 1000 --extend-right-m 1000`.
Older maps and previews moved to `png/archive/`.

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
                                           subdivision (STRtree, parallel, per-tile cache,
                                           RAM guard) + parallel dissolve
    v
05_building_values.py --variant V       -- (optional) best frequency per building
    v
render_qgis_style.py                    -- Standalone re-implementation of the QGIS layouts
      [--residents] [--buildings] [--palette] [--crop lon,lat,w,h] [--extend-left/right-m]
      (layer cache in cache/render/, rasterio drawing; a city renders in ~1-7 min)
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
  explicit tags win; untagged gates are closed except within 15 m of parks/cemeteries;
  lift gates ignored; ROD allotments count as closed.

## Quick start (new city)

```bash
cd script
python 00_download_gtfs.py --city poznan
python 01_calculate_trip_counts.py --city poznan YYYYMMDD
python 02_fetch_walking_network.py --city poznan
python 03_generate_isochrones_local.py --city poznan [--gates]
python 04_create_coverage_map.py --city poznan [--variant gates]
python D:\QGIS\osm_basemap\fetch_osm_basemap.py --city poznan
python render_qgis_style.py --city poznan --coverage ../_data/poznan/<folder>/coverage_map.gpkg \
    --date DD.MM.YYYY --out ../png/<name>.png
```

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
