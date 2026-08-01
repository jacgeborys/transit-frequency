# Transit Frequency Map

Visualizes transit accessibility in Warsaw by combining GTFS schedule data with walking isochrones.

## Status

**Overhauled (2026-08):** Replaced GraphHopper API isochrones with local OSMnx network analysis.
Added route-level deduplication — overlapping isochrones from nearby stops no longer double-count
the same transit line.

## Architecture

```
GTFS data (_data/YYYY_MM_DD/{ZTM,KM,WKD,metro}/)
    |
    v
01_*.py  -- Count trips & unique routes per stop
    |
    v
02_fetch_walking_network.py  -- OSM walking network (cached locally)
    |
    v
03_generate_isochrones_local.py  -- 5-min walking polygons (ego_graph + buffer)
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

## Data sources

- **GTFS**: ZTM, KM, WKD feeds (via transit.land)
- **Walking network**: OpenStreetMap (via OSMnx)
