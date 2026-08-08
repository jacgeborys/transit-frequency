"""
Calculate trip counts per stop from GTFS data.
Uses calendar_dates.txt for date filtering and city-specific vehicle classification.

Outputs a single CSV with all operators, vehicle types, unique routes, and route_ids.

Usage:
    python 01_calculate_trip_counts.py --city warsaw YYYYMMDD [data_folder]
    python 01_calculate_trip_counts.py --city poznan YYYYMMDD

    If data_folder is omitted, uses the most recent folder in _data/<city>/.
"""
import sys
import argparse
from pathlib import Path
from datetime import time

import pandas as pd
import numpy as np

from cities import get_city, add_city_argument

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# Time window: 6 AM - 10 PM
TIME_START = time(6, 0)
TIME_END = time(22, 0)


def parse_gtfs_time(time_str: str):
    """Parse GTFS time, handling hours >= 24. Returns time object or None."""
    try:
        parts = time_str.split(':')
        hour = int(parts[0]) % 24
        return time(hour, int(parts[1]), int(parts[2]))
    except (ValueError, AttributeError):
        return None


def expand_frequencies(data_dir: Path, active_service_ids: list, classify_fn) -> pd.DataFrame:
    """Expand frequencies.txt into synthetic stop_times rows."""
    freq_path = data_dir / "frequencies.txt"
    if not freq_path.exists():
        return pd.DataFrame()

    frequencies = pd.read_csv(freq_path)
    if frequencies.empty:
        return pd.DataFrame()

    trips = pd.read_csv(data_dir / "trips.txt", dtype=str)
    template_ids = frequencies['trip_id'].unique()
    freq_templates = trips[trips['trip_id'].isin(template_ids)].copy()

    # Filter to active services
    # For Warsaw metro: short codes like 'PcM' that don't start with '20'
    freq_svc = {s for s in active_service_ids if not s.startswith('20')}
    if freq_svc:
        freq_templates = freq_templates[freq_templates['service_id'].isin(freq_svc)]
        frequencies = frequencies[frequencies['trip_id'].isin(freq_templates['trip_id'])]

    if freq_templates.empty:
        return pd.DataFrame()

    stop_times = pd.read_csv(data_dir / "stop_times.txt", dtype={'stop_id': str})
    freq_stop_times = stop_times[stop_times['trip_id'].isin(template_ids)]

    def _secs(t):
        h, m, s = t.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

    def _fmt(secs):
        h, rem = divmod(int(secs), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    rows = []
    for _, freq in frequencies.iterrows():
        tmpl = freq_templates[freq_templates['trip_id'] == freq['trip_id']]
        if tmpl.empty:
            continue
        tmpl = tmpl.iloc[0]
        route_id = str(tmpl['route_id'])

        tmpl_stops = freq_stop_times[freq_stop_times['trip_id'] == freq['trip_id']]

        start = _secs(freq['start_time'])
        end = _secs(freq['end_time'])
        headway = int(freq['headway_secs'])

        t = start
        while t < end:
            dep_str = _fmt(t)
            for _, st in tmpl_stops.iterrows():
                rows.append({
                    'trip_id': f"{freq['trip_id']}__{t}",
                    'stop_id': st['stop_id'],
                    'arrival_time': dep_str,
                    'departure_time': dep_str,
                    'route_id': route_id,
                })
            t += headway

    print(f"  Frequencies: {len(rows)} synthetic stop_time entries")
    return pd.DataFrame(rows)


def find_latest_data_dir(city: dict) -> Path:
    """Find the most recent data folder for a city."""
    base = city['data_dir']
    if not base.exists():
        raise FileNotFoundError(f"No data directory for {city['name']}: {base}")
    data_dirs = sorted(
        [d for d in base.iterdir() if d.is_dir() and (d / 'stops.txt').exists()],
        key=lambda x: x.name, reverse=True
    )
    if not data_dirs:
        raise FileNotFoundError(f"No GTFS data in {base}. Run 00_download_gtfs.py first.")
    return data_dirs[0]


def main():
    parser = argparse.ArgumentParser(description='Calculate trip counts per stop')
    add_city_argument(parser)
    parser.add_argument('date', nargs='?', help='Target date YYYYMMDD (default: auto-pick best weekday)')
    parser.add_argument('data_folder', nargs='?', help='Data folder path (default: most recent)')
    args = parser.parse_args()

    city = get_city(args.city)
    classify_vehicle = city['vehicle_classify']

    data_dir = Path(args.data_folder) if args.data_folder else find_latest_data_dir(city)

    print("=" * 60)
    print(f"Trip Count Calculator — {city['name']}")
    print("=" * 60)
    print(f"Data: {data_dir}\n")

    # --- Load data ---
    print("Loading GTFS files...")
    stops_df = pd.read_csv(data_dir / "stops.txt", dtype={'stop_id': str})
    stop_times_df = pd.read_csv(data_dir / "stop_times.txt", dtype={'stop_id': str})
    trips_df = pd.read_csv(data_dir / "trips.txt", dtype={'route_id': str, 'service_id': str})
    calendar_df = pd.read_csv(data_dir / "calendar_dates.txt", dtype={'service_id': str})
    calendar_df['date'] = calendar_df['date'].astype(int)
    calendar_df['exception_type'] = calendar_df['exception_type'].astype(int)

    # --- Determine target date ---
    if args.date:
        target_date = args.date
    else:
        # Auto-pick: first available Monday (weekday with most service)
        active_dates = calendar_df[calendar_df['exception_type'] == 1]
        services_per_date = active_dates.groupby('date')['service_id'].nunique().reset_index()
        services_per_date.columns = ['date', 'services']
        services_per_date = services_per_date.sort_values('services', ascending=False)
        target_date = str(services_per_date.iloc[0]['date'])
        print(f"Auto-selected date: {target_date} ({services_per_date.iloc[0]['services']} services)")

    print(f"Target date: {target_date}\n")

    # --- Filter by date ---
    active_services = calendar_df[
        (calendar_df['date'] == int(target_date)) &
        (calendar_df['exception_type'] == 1)
    ]['service_id'].tolist()
    print(f"Active service_ids for {target_date}: {len(active_services)}")

    # Substring matching (handles ZTM's '2026-08-03:PcS' format)
    mask = trips_df['service_id'].apply(
        lambda sid: any(s in str(sid) for s in active_services)
    )
    trips_filtered = trips_df[mask].copy()
    print(f"Matching trips: {len(trips_filtered)}")

    # Map trip_id -> route_id
    trip_route = trips_filtered[['trip_id', 'route_id']].drop_duplicates()

    # --- Merge route_id into stop_times, filter by active trips ---
    stop_times_filtered = stop_times_df[
        stop_times_df['trip_id'].isin(trips_filtered['trip_id'])
    ].copy()
    stop_times_filtered = stop_times_filtered.merge(trip_route, on='trip_id', how='left')

    # --- Expand frequencies (metro, S-Bahn, etc.) ---
    if city.get('has_frequencies'):
        freq_rows = expand_frequencies(data_dir, active_services, classify_vehicle)
        if not freq_rows.empty:
            stop_times_filtered = pd.concat([stop_times_filtered, freq_rows], ignore_index=True)

    # --- Filter by time (6 AM - 10 PM) ---
    stop_times_filtered['time_parsed'] = stop_times_filtered['arrival_time'].apply(parse_gtfs_time)
    stop_times_filtered = stop_times_filtered.dropna(subset=['time_parsed'])
    stop_times_filtered = stop_times_filtered[
        (stop_times_filtered['time_parsed'] >= TIME_START) &
        (stop_times_filtered['time_parsed'] < TIME_END)
    ]
    print(f"Stop times in {TIME_START}-{TIME_END}: {len(stop_times_filtered)}")

    # --- Classify vehicles ---
    stop_times_filtered['vehicle'] = stop_times_filtered['route_id'].apply(classify_vehicle)

    # --- Aggregate per stop ---
    vehicle_counts = (
        stop_times_filtered
        .groupby(['stop_id', 'vehicle'])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    unique_routes = (
        stop_times_filtered
        .groupby('stop_id')['route_id']
        .nunique()
        .reset_index(name='unique_routes')
    )

    route_lists = (
        stop_times_filtered
        .groupby('stop_id')['route_id']
        .apply(lambda x: ','.join(sorted(x.unique())))
        .reset_index(name='route_ids')
    )

    route_trip_counts = (
        stop_times_filtered
        .groupby(['stop_id', 'route_id'])
        .size()
        .reset_index(name='count')
        .groupby('stop_id')
        .apply(lambda g: ','.join(f"{r}:{c}" for r, c in zip(g['route_id'], g['count'])))
        .reset_index(name='route_trip_counts')
    )

    # --- Merge into stops ---
    result = stops_df.merge(vehicle_counts, on='stop_id', how='inner')
    result = result.merge(unique_routes, on='stop_id', how='left')
    result = result.merge(route_lists, on='stop_id', how='left')
    result = result.merge(route_trip_counts, on='stop_id', how='left')

    for col in ['bus', 'tram', 'train', 'metro']:
        if col not in result.columns:
            result[col] = 0
        else:
            result[col] = result[col].astype(int)

    result['unique_routes'] = result['unique_routes'].fillna(0).astype(int)
    result['trip_count'] = result['bus'] + result['tram'] + result['train'] + result['metro']

    result = result[result['trip_count'] > 0]

    # --- Save ---
    output_path = data_dir / "stops_trip_count.csv"
    result.to_csv(output_path, index=False)

    print(f"\n{'='*60}")
    print(f"Results: {len(result)} stops with service")
    print(f"{'='*60}")
    print(f"  Bus:    {result['bus'].sum():>7,} departures")
    print(f"  Tram:   {result['tram'].sum():>7,} departures")
    print(f"  Train:  {result['train'].sum():>7,} departures")
    print(f"  Metro:  {result['metro'].sum():>7,} departures")
    print(f"  Total:  {result['trip_count'].sum():>7,} departures")
    print(f"  Unique routes across all stops: {stop_times_filtered['route_id'].nunique()}")
    print(f"\nSaved to: {output_path}")


if __name__ == '__main__':
    main()
