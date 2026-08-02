## Pipeline

Run scripts in numbered order:

0. **00_download_gtfs.py** — Download and merge ZTM + KM + WKD feeds from mkuran.pl
   into a single GTFS directory. Metro is included in ZTM's feed (frequencies.txt).
   ```
   python 00_download_gtfs.py              # download + show available dates
   python 00_download_gtfs.py --dates-only  # check dates in existing data
   ```

1. **01_calculate_trip_counts.py** — Count trips and unique routes per stop (all operators).
   Uses calendar_dates.txt for date filtering, classifies vehicles, expands metro frequencies.
   ```
   python 01_calculate_trip_counts.py 20260824
   ```

2. **02_fetch_walking_network.py** — Download/cache the OSM walking network for Warsaw via OSMnx.
   Will reuse the zabka project's cached network if available.

3. **03_generate_isochrones_local.py** — Generate 5-minute walking isochrones per stop using the
   local network graph (ego_graph + node/edge buffering). No API keys needed.
   ```
   python 03_generate_isochrones_local.py
   ```

### Archived

Old per-operator scripts (01_calculate_trip_count_for_stop_*.py) and GraphHopper API
isochrone scripts are in `archive/`. Replaced by the unified pipeline above.
