# Transit Frequency Map

Visualizes transit accessibility by combining GTFS schedule data with walking isochrones.
Supports multiple cities — all scripts accept `--city <name>`.

## Supported cities

| City | GTFS source | Notes |
|------|------------|-------|
| **Warsaw** (default) | ZTM + KM + WKD via mkuran.pl | Multi-feed merge |
| **Poznan** | ZTM Poznan via mkuran.pl | Single feed |
| **Krakow** | ZTM Krakow via mkuran.pl | Single feed |
| **Gdansk** (Tricity) | ZTM Gdansk via mkuran.pl | Includes Gdynia + Sopot |
| **Berlin** | VBB GTFS | U/S-Bahn, trams, buses |

## Status

**Multi-city support (2026-08):** Refactored entire pipeline to accept `--city` argument.
Added generalized OSM basemap fetcher. City configs in `script/cities.py`.

**Current Warsaw data:** Aug 24, 2026 (Monday) — 7,053 stops, 309 routes, 771k departures.

## Architecture

```
00_download_gtfs.py --city <name>    -- Download GTFS (auto-merge for Warsaw)
    |
    v
_data/<city>/YYYY_MM_DD/             -- Merged GTFS directory
    |
    v
01_calculate_trip_counts.py --city   -- Count trips & unique routes per stop
    |
    v
02_fetch_walking_network.py --city   -- OSM walking network (cached via OSMnx)
    |
    v
03_generate_isochrones_local.py      -- 5-min walking polygons
    |
    v
04_create_coverage_map.py --city     -- Deduplicated frequency per area
    |
    v
render_map.py --city                 -- Dark-theme PNG render

fetch_osm_basemap.py --city          -- Basemap layers (water, parks, roads, etc.)
```

## Quick start (new city)

```bash
cd script
python 00_download_gtfs.py --city poznan
python 01_calculate_trip_counts.py --city poznan
python 02_fetch_walking_network.py --city poznan
python 03_generate_isochrones_local.py --city poznan
python 04_create_coverage_map.py --city poznan
python fetch_osm_basemap.py --city poznan
python render_map.py --city poznan
```

## Directory structure

```
_data/<city>/YYYY_MM_DD/   -- GTFS data + outputs (per city, per date)
network/<city>/            -- Walking network cache
basemap/<city>/            -- OSM basemap layers (water, parks, roads, buildings, etc.)
styles/                    -- QGIS QML styles
png/                       -- Rendered maps
```

## Key concepts

- **trip_count**: Total departures at a stop (6 AM - 10 PM)
- **unique_routes**: Number of distinct transit lines serving a stop
- **route_ids**: Comma-separated list of route IDs per stop/isochrone, enabling
  deduplication when isochrones from nearby stops overlap
- **Isochrones**: 5-min walking polygons built by buffering reachable network
  nodes and edges (50 m buffer, 4.5 km/h walking speed)
- **Vehicle types**: bus, tram, train, metro — classified by city-specific route_id rules

## Data sources

- **GTFS**: [mkuran.pl](https://mkuran.pl/gtfs/) (Polish cities), VBB (Berlin)
- **Walking network**: OpenStreetMap (via OSMnx)
- **Basemap**: OpenStreetMap (via Overpass API + OSMnx)
