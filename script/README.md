## Pipeline

Run scripts in numbered order:

1. **01_calculate_trip_count_*.py** — Process GTFS data to count trips and unique routes per stop.
   Each stop gets `trip_count` (total departures), `unique_routes`, and `route_ids` (comma-separated).
   - `01_calculate_trip_count_for_stop_ztm.py` — ZTM (bus, tram, train)
   - `01_calculate_trip_count_for_stop_km.py` — KM regional rail (with DBSCAN clustering)
   - `01_calculate_trip_count_for_stop_wkd.py` — WKD suburban rail
   - `01_calculate_trip_count_based_on_frequency_metro.py` — Metro (headway-based)

2. **02_fetch_walking_network.py** — Download/cache the OSM walking network for Warsaw via OSMnx.
   Will reuse the zabka project's cached network if available.

3. **03_generate_isochrones_local.py** — Generate 5-minute walking isochrones per stop using the
   local network graph (ego_graph + node/edge buffering). No API keys needed.
   ```
   python 03_generate_isochrones_local.py ztm    # single operator
   python 03_generate_isochrones_local.py all     # all operators
   ```

### Archived

Old GraphHopper API-based isochrone scripts are in `archive/`. These required
API keys and were rate-limited. Replaced by local OSMnx approach.
