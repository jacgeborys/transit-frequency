"""
City configurations for multi-city transit frequency maps.

Each city defines:
- bbox: bounding box (south, west, north, east) in EPSG:4326
- crs_metric: metric CRS for geometry operations
- gtfs: dict of {feed_name: url} for GTFS downloads
- gtfs_merge: how to merge multiple feeds (optional)
- vehicle_rules: route_id -> vehicle type classification
- osm_network_tags: Overpass transit network filter tags (optional)
- render_extent: EPSG:2180 or metric CRS extent for render_map (optional)

Usage:
    from cities import get_city, list_cities
    city = get_city('poznan')
"""
import argparse
import re
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Vehicle classification helpers
# ---------------------------------------------------------------------------

def _warsaw_vehicle(rid):
    rid = str(rid).strip()
    if rid.startswith('M') and len(rid) >= 2 and rid[1:].isdigit():
        return 'metro'
    if rid.isdigit() and len(rid) <= 2:
        return 'tram'
    if rid.isdigit() and len(rid) == 3:
        return 'bus'
    if (rid.startswith(('S', 'R'))
            or (rid.startswith('KM_R') and '_BUS' not in rid)
            or (rid.startswith('wkd_') and 'bus' not in rid)):
        return 'train'
    return 'bus'


def _poznan_vehicle(rid):
    rid = str(rid).strip()
    # Poznań trams: 1-99
    if rid.isdigit() and int(rid) < 100:
        return 'tram'
    # Poznań trains: KW lines (Koleje Wielkopolskie)
    if rid.upper().startswith(('S', 'R', 'KW')):
        return 'train'
    return 'bus'


def _krakow_vehicle(rid):
    rid = str(rid).strip()
    # Kraków trams: 1-99
    if rid.isdigit() and int(rid) < 100:
        return 'tram'
    # SKA (Szybka Kolej Aglomeracyjna)
    if rid.upper().startswith(('S', 'R', 'SKA')):
        return 'train'
    return 'bus'


def _gdansk_vehicle(rid):
    rid = str(rid).strip()
    # Tricity trams: 1-99 (Gdańsk trams)
    if rid.isdigit() and int(rid) < 100:
        return 'tram'
    # SKM (Szybka Kolej Miejska), PKM (Pomorska Kolej Metropolitalna)
    if rid.upper().startswith(('S', 'R', 'SKM', 'PKM')):
        return 'train'
    return 'bus'


def _berlin_vehicle(rid):
    rid = str(rid).strip()
    # U-Bahn
    if re.match(r'^U\d', rid):
        return 'metro'
    # S-Bahn
    if re.match(r'^S\d', rid):
        return 'train'
    # Trams: M-lines and numbered < 100
    if re.match(r'^M\d', rid) or (rid.isdigit() and int(rid) < 100):
        return 'tram'
    return 'bus'


# ---------------------------------------------------------------------------
# City definitions
# ---------------------------------------------------------------------------

CITIES = {
    'warsaw': {
        'name': 'Warszawa',
        'bbox': {
            'south': 52.0977, 'west': 20.8519,
            'north': 52.3690, 'east': 21.2711,
        },
        'crs_metric': 'EPSG:2180',
        'gtfs': {
            'ztm': 'https://mkuran.pl/gtfs/warsaw.zip',
            'polish_trains': 'https://mkuran.pl/gtfs/polish_trains.zip',
            'wkd': 'https://mkuran.pl/gtfs/wkd.zip',
        },
        'gtfs_merge': 'warsaw',  # special merge logic for ZTM+KM+WKD
        'vehicle_classify': _warsaw_vehicle,
        'has_frequencies': True,  # metro uses frequencies.txt
        'render_extent': (623233.8, 651733.8, 477610.7, 500410.7),
    },

    'poznan': {
        'name': 'Poznań',
        'bbox': {
            'south': 52.30, 'west': 16.73,
            'north': 52.50, 'east': 17.10,
        },
        'crs_metric': 'EPSG:2180',
        'gtfs': {
            'ztm': 'https://www.ztm.poznan.pl/pl/dla-deweloperow/getGTFSFile',
        },
        'gtfs_merge': 'single',
        'vehicle_classify': _poznan_vehicle,
        'has_frequencies': False,
    },

    'krakow': {
        'name': 'Kraków',
        'bbox': {
            'south': 49.97, 'west': 19.79,
            'north': 50.13, 'east': 20.13,
        },
        'crs_metric': 'EPSG:2180',
        'gtfs': {
            # A=autobus, T=tram, M=mobilis (outsourced bus operator)
            'bus': 'https://gtfs.ztp.krakow.pl/GTFS_KRK_A.zip',
            'tram': 'https://gtfs.ztp.krakow.pl/GTFS_KRK_T.zip',
        },
        'gtfs_merge': 'krakow',  # merge bus + tram feeds
        'vehicle_classify': _krakow_vehicle,
        'has_frequencies': False,
    },

    'gdansk': {
        'name': 'Trójmiasto',
        'bbox': {
            'south': 54.28, 'west': 18.45,
            'north': 54.52, 'east': 18.82,
        },
        'crs_metric': 'EPSG:2180',
        'gtfs': {
            'ztm': 'https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip',
        },
        'gtfs_merge': 'single',
        'vehicle_classify': _gdansk_vehicle,
        'has_frequencies': False,
    },

    'berlin': {
        'name': 'Berlin',
        'bbox': {
            'south': 52.34, 'west': 13.09,
            'north': 52.68, 'east': 13.76,
        },
        'crs_metric': 'EPSG:25833',
        'gtfs': {
            'vbb': 'https://www.vbb.de/fileadmin/user_upload/VBB/Dokumente/API-Datensaetze/gtfs-mastscharf/GTFS.zip',
        },
        'gtfs_merge': 'single',
        'vehicle_classify': _berlin_vehicle,
        'has_frequencies': True,
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_city(name: str) -> dict:
    """Get city config by name (case-insensitive). Raises KeyError if not found."""
    key = name.lower().strip()
    if key not in CITIES:
        available = ', '.join(sorted(CITIES.keys()))
        raise KeyError(f"Unknown city '{name}'. Available: {available}")
    city = CITIES[key].copy()
    city['key'] = key
    city['data_dir'] = PROJECT_DIR / "_data" / key
    city['network_dir'] = PROJECT_DIR / "network" / key
    city['osm_dir'] = PROJECT_DIR / "basemap" / key
    return city


def list_cities() -> list:
    """Return list of (key, name) tuples."""
    return [(k, v['name']) for k, v in CITIES.items()]


def add_city_argument(parser: argparse.ArgumentParser):
    """Add --city argument to an argparse parser."""
    available = ', '.join(sorted(CITIES.keys()))
    parser.add_argument(
        '--city', type=str, default='warsaw',
        help=f'City to process (default: warsaw). Available: {available}',
    )
    return parser
