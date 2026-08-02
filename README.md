# Transit Frequency Map

Visualizes transit accessibility in Warsaw by combining GTFS schedule data with walking isochrones.

## Status

**Overhauled (2026-08):** Replaced GraphHopper API isochrones with local OSMnx network analysis.
Added route-level deduplication — overlapping isochrones from nearby stops no longer double-count
the same transit line. Unified all operators (ZTM, KM, WKD, Metro) into a single pipeline.

**Current data:** Aug 24, 2026 (Monday) — 7,053 stops, 309 routes, 771k departures.

## Architecture

```
00_download_gtfs.py  -- Download & merge ZTM + KM + WKD from mkuran.pl
    |
    v
_data/YYYY_MM_DD/  (single merged GTFS directory)
    |
    v
01_calculate_trip_counts.py  -- Count trips & unique routes per stop (all operators)
    |
    v
02_fetch_walking_network.py  -- OSM walking network (cached locally via OSMnx)
    |
    v
03_generate_isochrones_local.py  -- 5-min walking polygons (ego_graph + node/edge buffer)
    |
    v
QGIS (transit-frequency-map.qgz)  -- Overlay, aggregate, visualize
```

## Key concepts

- **trip_count**: Total departures at a stop (6 AM - 10 PM)
- **unique_routes**: Number of distinct transit lines serving a stop
- **route_ids**: Comma-separated list of route IDs per stop/isochrone, enabling
  deduplication when isochrones from nearby stops overlap
- **Isochrones**: 5-min walking polygons built by buffering reachable network
  nodes and edges (50 m buffer, 4.5 km/h walking speed)
- **Vehicle types**: bus, tram, train (KM/WKD/SKM), metro — classified by route_id rules

## Data sources

- **GTFS**: ZTM, KM, WKD feeds via [mkuran.pl](https://mkuran.pl/gtfs/)
- **Walking network**: OpenStreetMap (via OSMnx)
