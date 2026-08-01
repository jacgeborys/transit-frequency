"""
Download and merge GTFS feeds for Warsaw transit.
Sources: mkuran.pl (ZTM, Koleje Mazowieckie via polish_trains, WKD).
Metro is included in ZTM's feed (frequencies.txt).

After downloading, prints available date ranges so you can pick an analysis date.

Usage:
    python 00_download_gtfs.py                # download + show available dates
    python 00_download_gtfs.py --dates-only   # just check dates in existing data
"""
import sys
import shutil
import zipfile
import logging
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import requests

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR.parent / "_data"

# mkuran.pl feed URLs
GTFS_URLS = {
    'ztm': 'https://mkuran.pl/gtfs/warsaw.zip',
    'polish_trains': 'https://mkuran.pl/gtfs/polish_trains.zip',
    'wkd': 'https://mkuran.pl/gtfs/wkd.zip',
}

KM_PREFIX = 'km_'
WKD_PREFIX = 'wkd_'


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

def download_and_extract(url: str, name: str, output_dir: Path) -> Path:
    """Download a GTFS zip, extract, return path to extracted dir."""
    output_dir.mkdir(parents=True, exist_ok=True)
    zip_path = output_dir / f'{name}.zip'

    logger.info(f'Downloading {name} from {url}')
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total = int(response.headers.get('content-length', 0))
    downloaded = 0
    with open(zip_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            downloaded += len(chunk)
            f.write(chunk)
            if total:
                print(f'\r  {name}: {downloaded / total * 100:.1f}%', end='')
    print()

    extract_dir = output_dir / name
    if extract_dir.exists():
        shutil.rmtree(extract_dir)
    extract_dir.mkdir()
    with zipfile.ZipFile(zip_path, 'r') as zf:
        zf.extractall(extract_dir)
    zip_path.unlink()

    logger.info(f'{name}: {len(list(extract_dir.glob("*.txt")))} files extracted')
    return extract_dir


# ---------------------------------------------------------------------------
# Calendar helpers
# ---------------------------------------------------------------------------

def expand_calendar_txt(calendar_df: pd.DataFrame) -> pd.DataFrame:
    """Convert calendar.txt (day-of-week patterns) to calendar_dates.txt format."""
    day_cols = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    rows = []
    for _, row in calendar_df.iterrows():
        start = datetime.strptime(str(int(row['start_date'])), '%Y%m%d')
        end = datetime.strptime(str(int(row['end_date'])), '%Y%m%d')
        current = start
        while current <= end:
            if int(row[day_cols[current.weekday()]]) == 1:
                rows.append({
                    'service_id': row['service_id'],
                    'date': int(current.strftime('%Y%m%d')),
                    'exception_type': 1,
                })
            current += timedelta(days=1)
    return pd.DataFrame(rows)


def load_calendar_dates(gtfs_dir: Path, service_ids: set) -> pd.DataFrame:
    """Load calendar entries, handling both calendar.txt and calendar_dates.txt."""
    if (gtfs_dir / 'calendar_dates.txt').exists():
        df = pd.read_csv(gtfs_dir / 'calendar_dates.txt', dtype=str)
        df['date'] = df['date'].astype(int)
        df['exception_type'] = df['exception_type'].astype(int)
        return df[df['service_id'].isin(service_ids)].copy()
    elif (gtfs_dir / 'calendar.txt').exists():
        cal = pd.read_csv(gtfs_dir / 'calendar.txt', dtype=str)
        return expand_calendar_txt(cal[cal['service_id'].isin(service_ids)])
    else:
        raise FileNotFoundError(f'No calendar file in {gtfs_dir}')


# ---------------------------------------------------------------------------
# KM extraction (from polish_trains, filter by Mazowieckie agency)
# ---------------------------------------------------------------------------

def extract_km_data(trains_dir: Path) -> dict:
    """Extract Koleje Mazowieckie from polish_trains feed, apply km_ prefix."""
    agency = pd.read_csv(trains_dir / 'agency.txt', dtype=str)
    km_row = agency[agency['agency_name'].str.contains('Mazowieckie', case=False, na=False)]
    if km_row.empty:
        raise ValueError(f'No Koleje Mazowieckie in {trains_dir}/agency.txt')
    km_agency_id = str(km_row.iloc[0]['agency_id'])

    routes = pd.read_csv(trains_dir / 'routes.txt', dtype=str)
    km_route_ids = set(routes.loc[routes['agency_id'] == km_agency_id, 'route_id'])
    logger.info(f'  KM routes: {sorted(km_route_ids)}')

    trips = pd.read_csv(trains_dir / 'trips.txt', dtype=str)
    km_trips = trips[trips['route_id'].isin(km_route_ids)].copy()
    km_trip_ids = set(km_trips['trip_id'])
    km_shape_ids = set(km_trips['shape_id'].dropna())
    km_service_ids = set(km_trips['service_id'])

    shapes = pd.read_csv(trains_dir / 'shapes.txt', dtype=str)
    km_shapes = shapes[shapes['shape_id'].isin(km_shape_ids)].copy()

    stop_times = pd.read_csv(trains_dir / 'stop_times.txt', dtype=str)
    km_stop_times = stop_times[stop_times['trip_id'].isin(km_trip_ids)].copy()
    km_stop_ids = set(km_stop_times['stop_id'])

    stops = pd.read_csv(trains_dir / 'stops.txt', dtype=str)
    km_stops = stops[stops['stop_id'].isin(km_stop_ids)].copy()

    km_cal = load_calendar_dates(trains_dir, km_service_ids)

    # Prefix IDs (route_id kept as-is — already distinctive like R1, RE2)
    km_trips['trip_id'] = KM_PREFIX + km_trips['trip_id']
    km_trips['shape_id'] = KM_PREFIX + km_trips['shape_id']
    km_trips['service_id'] = KM_PREFIX + km_trips['service_id']
    km_shapes['shape_id'] = KM_PREFIX + km_shapes['shape_id']
    km_stop_times['trip_id'] = KM_PREFIX + km_stop_times['trip_id']
    km_stop_times['stop_id'] = KM_PREFIX + km_stop_times['stop_id']
    km_stops['stop_id'] = KM_PREFIX + km_stops['stop_id']
    km_cal['service_id'] = KM_PREFIX + km_cal['service_id'].astype(str)

    return {
        'trips': km_trips, 'shapes': km_shapes,
        'stop_times': km_stop_times, 'stops': km_stops,
        'calendar_dates': km_cal,
    }


# ---------------------------------------------------------------------------
# WKD extraction
# ---------------------------------------------------------------------------

def extract_wkd_data(wkd_dir: Path) -> dict:
    """Extract WKD data, apply wkd_ prefix. Rail routes vs bus replacements."""
    routes = pd.read_csv(wkd_dir / 'routes.txt', dtype=str)
    rail_ids = set(routes.loc[routes['route_type'].isin(['0', '1', '2']), 'route_id'])
    bus_ids = set(routes['route_id']) - rail_ids

    trips = pd.read_csv(wkd_dir / 'trips.txt', dtype=str)
    wkd_trip_ids = set(trips['trip_id'])
    wkd_shape_ids = set(trips['shape_id'].dropna())
    wkd_service_ids = set(trips['service_id'])

    shapes = pd.read_csv(wkd_dir / 'shapes.txt', dtype=str)
    wkd_shapes = shapes[shapes['shape_id'].isin(wkd_shape_ids)].copy()

    stop_times = pd.read_csv(wkd_dir / 'stop_times.txt', dtype=str)
    wkd_stop_times = stop_times[stop_times['trip_id'].isin(wkd_trip_ids)].copy()
    wkd_stop_ids = set(wkd_stop_times['stop_id'])

    stops = pd.read_csv(wkd_dir / 'stops.txt', dtype=str)
    wkd_stops = stops[stops['stop_id'].isin(wkd_stop_ids)].copy()

    wkd_cal = load_calendar_dates(wkd_dir, wkd_service_ids)

    def _route_prefix(route_id):
        return WKD_PREFIX if route_id in rail_ids else WKD_PREFIX + 'bus_'

    trips['route_id'] = trips['route_id'].apply(_route_prefix) + trips['route_id']
    trips['trip_id'] = WKD_PREFIX + trips['trip_id']
    trips['shape_id'] = WKD_PREFIX + trips['shape_id']
    trips['service_id'] = WKD_PREFIX + trips['service_id']
    wkd_shapes['shape_id'] = WKD_PREFIX + wkd_shapes['shape_id']
    wkd_stop_times['trip_id'] = WKD_PREFIX + wkd_stop_times['trip_id']
    wkd_stop_times['stop_id'] = WKD_PREFIX + wkd_stop_times['stop_id']
    wkd_stops['stop_id'] = WKD_PREFIX + wkd_stops['stop_id']
    wkd_cal['service_id'] = WKD_PREFIX + wkd_cal['service_id'].astype(str)

    return {
        'trips': trips, 'shapes': wkd_shapes,
        'stop_times': wkd_stop_times, 'stops': wkd_stops,
        'calendar_dates': wkd_cal,
    }


# ---------------------------------------------------------------------------
# Merge feeds
# ---------------------------------------------------------------------------

def append_to_combined(combined_dir: Path, data: dict, label: str):
    """Append a feed's DataFrames into the combined directory files."""
    file_map = {
        'trips': 'trips.txt', 'shapes': 'shapes.txt',
        'stop_times': 'stop_times.txt', 'stops': 'stops.txt',
        'calendar_dates': 'calendar_dates.txt',
    }
    for key, filename in file_map.items():
        target = combined_dir / filename
        base_df = pd.read_csv(target, dtype=str)
        new_df = data[key].astype(str)
        common = [c for c in base_df.columns if c in new_df.columns]
        merged = pd.concat([base_df, new_df[common]], ignore_index=True)
        merged.to_csv(target, index=False)
        logger.info(f'  {filename}: +{len(new_df)} {label} rows -> {len(merged)} total')


# ---------------------------------------------------------------------------
# Date availability checker
# ---------------------------------------------------------------------------

def show_available_dates(combined_dir: Path):
    """Print available service dates from the combined GTFS feed."""
    cal_file = combined_dir / 'calendar_dates.txt'
    if not cal_file.exists():
        print("No calendar_dates.txt found.")
        return

    cal = pd.read_csv(cal_file, dtype=str)
    cal['date'] = cal['date'].astype(int)
    # Only include added services (exception_type=1)
    cal = cal[cal['exception_type'] == '1']

    dates = sorted(cal['date'].unique())
    if not dates:
        print("No service dates found.")
        return

    # Parse to datetime for day-of-week info
    date_objs = [datetime.strptime(str(d), '%Y%m%d') for d in dates]
    day_names = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

    print(f"\n{'='*60}")
    print(f"Available service dates: {len(dates)}")
    print(f"Range: {dates[0]} to {dates[-1]}")
    print(f"{'='*60}")

    # Count services per date to find good analysis days
    services_per_date = cal.groupby('date')['service_id'].nunique().reset_index()
    services_per_date.columns = ['date', 'services']
    services_per_date = services_per_date.sort_values('services', ascending=False)

    # Show top dates by service count
    print(f"\nTop 10 dates by number of active services:")
    for _, row in services_per_date.head(10).iterrows():
        d = datetime.strptime(str(row['date']), '%Y%m%d')
        day = day_names[d.weekday()]
        print(f"  {row['date']} ({day}): {row['services']} services")

    # Check specific target date
    target = 20260902
    if target in dates:
        svc = int(services_per_date[services_per_date['date'] == target]['services'].iloc[0])
        d = datetime.strptime(str(target), '%Y%m%d')
        print(f"\n  Target {target} ({day_names[d.weekday()]}): AVAILABLE ({svc} services)")
    else:
        print(f"\n  Target {target}: NOT AVAILABLE")
        # Find nearest available weekday
        target_dt = datetime.strptime(str(target), '%Y%m%d')
        nearest = min(date_objs, key=lambda d: abs((d - target_dt).days))
        print(f"  Nearest available: {nearest.strftime('%Y%m%d')} ({day_names[nearest.weekday()]})")

    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    dates_only = '--dates-only' in sys.argv

    # Determine output directory
    timestamp = datetime.now().strftime('%Y_%m_%d')
    combined_dir = DATA_DIR / timestamp

    if dates_only:
        # Find most recent data folder
        data_dirs = sorted(
            [d for d in DATA_DIR.iterdir() if d.is_dir() and (d / 'stops.txt').exists()],
            key=lambda x: x.name, reverse=True
        )
        if not data_dirs:
            print("No existing GTFS data found. Run without --dates-only first.")
            return
        show_available_dates(data_dirs[0])
        return

    print("=" * 60)
    print("GTFS Download & Merge (ZTM + KM + WKD)")
    print("=" * 60)
    print(f"Output: {combined_dir}\n")

    if combined_dir.exists():
        print(f"Data folder {combined_dir} already exists.")
        print("Delete it to re-download, or use --dates-only to check dates.")
        show_available_dates(combined_dir)
        return

    # Download all feeds
    raw_dir = DATA_DIR / '_raw'
    ztm_dir = download_and_extract(GTFS_URLS['ztm'], 'ztm', raw_dir)
    trains_dir = download_and_extract(GTFS_URLS['polish_trains'], 'polish_trains', raw_dir)
    wkd_dir = download_and_extract(GTFS_URLS['wkd'], 'wkd', raw_dir)

    # Create combined directory from ZTM base
    combined_dir.mkdir(parents=True)
    for f in ztm_dir.glob('*.txt'):
        shutil.copy(f, combined_dir / f.name)
    logger.info(f'ZTM base copied to {combined_dir}')

    # Merge KM
    logger.info('Merging Koleje Mazowieckie...')
    append_to_combined(combined_dir, extract_km_data(trains_dir), 'KM')

    # Merge WKD
    logger.info('Merging WKD...')
    append_to_combined(combined_dir, extract_wkd_data(wkd_dir), 'WKD')

    # Clean up raw downloads
    shutil.rmtree(raw_dir)
    logger.info('Cleaned up raw downloads')

    print(f"\nCombined GTFS ready at: {combined_dir}")

    # Show available dates
    show_available_dates(combined_dir)


if __name__ == '__main__':
    main()
