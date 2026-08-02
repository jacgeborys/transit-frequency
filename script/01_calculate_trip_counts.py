"""
Calculate trip counts per stop from merged GTFS data (ZTM + KM + WKD + Metro).
Uses calendar_dates.txt for date filtering and substring matching for service_ids
(same approach as the animation pipeline).

Outputs a single CSV with all operators, vehicle types, unique routes, and route_ids.

Usage:
    python 01_calculate_trip_counts.py [YYYYMMDD] [data_folder]

    Defaults: date=20260824, folder=_data/2026_08_02
"""
import sys
from pathlib import Path
from datetime import time

import pandas as pd
import numpy as np

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# Defaults
DEFAULT_DATE = "20260824"
DEFAULT_DATA = PROJECT_DIR / "_data" / "2026_08_02"

# Vehicle classification (same rules as animation pipeline)
VEHICLE_TYPE_RULES = {
    'tram':  lambda rid: rid.isdigit() and len(rid) <= 2,
    'bus':   lambda rid: rid.isdigit() and len(rid) == 3,
    'train': lambda rid: (
        rid.startswith(('S', 'R'))
        or (rid.startswith('KM_R') and '_BUS' not in rid)
        or (rid.startswith('wkd_') and 'bus' not in rid)
    ),
    'metro': lambda rid: rid.startswith('M') and len(rid) >= 2 and rid[1:].isdigit(),
}

# Time window: 6 AM - 10 PM
TIME_START = time(6, 0)
TIME_END = time(22, 0)


def classify_vehicle(route_id: str) -> str:
    route_id = str(route_id).strip()
    for vtype, rule in VEHICLE_TYPE_RULES.items():
        if rule(route_id):
            return vtype
    return 'bus'


def parse_gtfs_time(time_str: str):
    """Parse GTFS time, handling hours >= 24. Returns time object or None."""
    try:
        parts = time_str.split(':')
        hour = int(parts[0]) % 24
        return time(hour, int(parts[1]), int(parts[2]))
    except (ValueError, AttributeError):
        return None


def expand_metro_frequencies(data_dir: Path, target_date: str, active_service_ids: list) -> pd.DataFrame:
    """Expand metro frequencies.txt into synthetic stop_times rows."""
    freq_path = data_dir / "frequencies.txt"
    if not freq_path.exists():
        return pd.DataFrame()

    frequencies = pd.read_csv(freq_path)
    metro_freq = frequencies[
        frequencies['trip_id'].str.startswith(('M1:', 'M2:'), na=False)
    ]
    if metro_freq.empty:
        return pd.DataFrame()

    # Filter to active metro service_ids (short codes like 'PcM')
    trips = pd.read_csv(data_dir / "trips.txt", dtype=str)
    template_ids = metro_freq['trip_id'].unique()
    metro_templates = trips[trips['trip_id'].isin(template_ids)].copy()

    metro_svc = {s for s in active_service_ids if not s.startswith('20')}
    if metro_svc:
        metro_templates = metro_templates[metro_templates['service_id'].isin(metro_svc)]
        metro_freq = metro_freq[metro_freq['trip_id'].isin(metro_templates['trip_id'])]

    if metro_templates.empty:
        return pd.DataFrame()

    # Load metro stops from stop_times to get stop_id + coordinates
    stop_times = pd.read_csv(data_dir / "stop_times.txt", dtype={'stop_id': str})
    metro_stop_times = stop_times[stop_times['trip_id'].isin(template_ids)]
    metro_stop_ids = metro_stop_times['stop_id'].unique()

    def _secs(t):
        h, m, s = t.split(':')
        return int(h) * 3600 + int(m) * 60 + int(s)

    def _fmt(secs):
        h, rem = divmod(int(secs), 3600)
        m, s = divmod(rem, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    rows = []
    for _, freq in metro_freq.iterrows():
        tmpl = metro_templates[metro_templates['trip_id'] == freq['trip_id']]
        if tmpl.empty:
            continue
        tmpl = tmpl.iloc[0]
        route_id = str(tmpl['route_id'])

        # Get stops for this template trip
        tmpl_stops = metro_stop_times[metro_stop_times['trip_id'] == freq['trip_id']]

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

    print(f"  Metro: {len(rows)} synthetic stop_time entries from frequencies.txt")
    return pd.DataFrame(rows)


def main():
    target_date = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_DATE
    data_dir = Path(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_DATA

    print("=" * 60)
    print(f"Trip Count Calculator — {target_date}")
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

    # --- Expand metro frequencies ---
    metro_rows = expand_metro_frequencies(data_dir, target_date, active_services)
    if not metro_rows.empty:
        stop_times_filtered = pd.concat([stop_times_filtered, metro_rows], ignore_index=True)

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
    # Total departures per vehicle type
    vehicle_counts = (
        stop_times_filtered
        .groupby(['stop_id', 'vehicle'])
        .size()
        .unstack(fill_value=0)
        .reset_index()
    )

    # Unique routes per stop
    unique_routes = (
        stop_times_filtered
        .groupby('stop_id')['route_id']
        .nunique()
        .reset_index(name='unique_routes')
    )

    # Route IDs as comma-separated string
    route_lists = (
        stop_times_filtered
        .groupby('stop_id')['route_id']
        .apply(lambda x: ','.join(sorted(x.unique())))
        .reset_index(name='route_ids')
    )

    # Per-route trip counts: "102:55,123:60,174:40"
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

    # Ensure vehicle columns exist
    for col in ['bus', 'tram', 'train', 'metro']:
        if col not in result.columns:
            result[col] = 0
        else:
            result[col] = result[col].astype(int)

    result['unique_routes'] = result['unique_routes'].fillna(0).astype(int)
    result['trip_count'] = result['bus'] + result['tram'] + result['train'] + result['metro']

    # Drop stops with no daytime service
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
