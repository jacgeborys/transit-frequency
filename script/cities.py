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
- qgis_group / qgis_layout: group and print layout in transit-frequency-map.qgz (optional)

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
    # Kraków GTFS uses "route_NNN" IDs — strip prefix for classification
    if rid.lower().startswith('route_'):
        rid = rid[6:]
    # Kraków trams: 1-99
    if rid.isdigit() and int(rid) < 100:
        return 'tram'
    # SKA (Szybka Kolej Aglomeracyjna)
    if rid.upper().startswith(('S', 'SKA')):
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


def _route_type_only(rid):
    # Feeds where every route has route_type (used first, see 01): this is only the fallback
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
        'geofabrik': 'https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf',
        # transit-frequency-map.qgz: layer-tree group under 'Tlo' + print layout
        'qgis_group': 'osm',
        'qgis_layout': 'how_many_rides_in_5_mins',
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
        'geofabrik': 'https://download.geofabrik.de/europe/poland/wielkopolskie-latest.osm.pbf',
        # transit-frequency-map.qgz: layer-tree group under 'Tlo' + print layout
        'qgis_group': 'poznan',
        'qgis_layout': 'how_many_rides_in_5_mins_poznan',
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
            'polish_trains': 'https://mkuran.pl/gtfs/polish_trains.zip',
        },
        'gtfs_merge': 'krakow',  # merge bus + tram + regional trains
        'vehicle_classify': _krakow_vehicle,
        'has_frequencies': False,
        # Regional train agencies to extract from polish_trains feed
        'train_agencies': {'KML', 'PR', 'KS'},  # Koleje Małopolskie, PolRegio, Koleje Śląskie
        'geofabrik': 'https://download.geofabrik.de/europe/poland/malopolskie-latest.osm.pbf',
        # transit-frequency-map.qgz: layer-tree group under 'Tlo' + print layout
        'qgis_group': 'krakow',
        'qgis_layout': 'how_many_rides_in_5_mins_krakow',
    },

    'gdansk': {
        'name': 'Gdańsk',
        'bbox': {
            'south': 54.30, 'west': 18.52,
            'north': 54.43, 'east': 18.80,
        },
        'crs_metric': 'EPSG:2180',
        'gtfs': {
            'ztm': 'https://ckan.multimediagdansk.pl/dataset/c24aa637-3619-4dc2-a171-a23eec8f2172/resource/30e783e4-2bec-4a7d-bb22-ee3e3b26ca96/download/gtfsgoogle.zip',
        },
        'gtfs_merge': 'single',
        'vehicle_classify': _gdansk_vehicle,
        'has_frequencies': False,
        'geofabrik': 'https://download.geofabrik.de/europe/poland/pomorskie-latest.osm.pbf',
        # transit-frequency-map.qgz: layer-tree group under 'Tlo' + print layout
        'qgis_group': 'gdansk',
        'qgis_layout': 'how_many_rides_in_5_mins_gdansk',
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
        'geofabrik': 'https://download.geofabrik.de/europe/germany/berlin-latest.osm.pbf',
        'network_tiles': 8,  # dense 45 x 38 km area: keep Overpass requests small
        'template': 'krakow',  # no QGIS layout of its own: render with Kraków's layout/styles
    },

    'manhattan': {
        'name': 'Manhattan',  # poster title "NEW YORK" (frame reaches deep into Brooklyn/Queens)
        # Covers the rotated render frame below (+ ~400 m so edge stops' isochrones are whole)
        'bbox': {
            'south': 40.599, 'west': -74.068,
            'north': 40.814, 'east': -73.792,
        },
        'crs_metric': 'EPSG:32618',  # UTM 18N (metres); NY State Plane is in US feet
        # Render rotated so the Hudson is vertical (oblique Mercator, gamma = rotation):
        # Hudson on the left edge, New Jersey left out (no NJ Transit data), Queens on the right
        # (gamma 69 = Hudson exactly vertical; 66 = island upright, Hudson leaning 3 deg)
        'render_crs': ('+proj=omerc +lat_0=40.745 +lonc=-73.975 +alpha=90 +gamma=66 +k_0=1 '
                       '+x_0=0 +y_0=0 +ellps=WGS84 +units=m +no_defs'),
        # m in render_crs: Upper Bay/Marine Park .. Central Park north, Hudson .. Fresh Meadows
        # (16.6 x 17.5 km since 2026-10-10: old 11.6 x 14.5 km frame + 3 km south, 5 km east;
        # corners checked to lie inside bbox)
        'render_frame': (-3100, -11100, 13500, 6370),
        'legend_corner': 'top-right',  # over Queens: bottom-left would cover Lower Manhattan
        'gtfs': {
            'subway': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_subway.zip',
            'bus_m': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_m.zip',
            'bus_bx': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_bx.zip',
            'bus_b': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_b.zip',
            'bus_q': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_q.zip',
            'bus_co': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfs_busco.zip',  # MTA Bus Co. (express)
            'lirr': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfslirr.zip',
            'mnr': 'https://rrgtfsfeeds.s3.amazonaws.com/gtfsmnr.zip',
            'path': 'http://data.trilliumtransit.com/gtfs/path-nj-us/path-nj-us.zip',
            'ferry': 'https://nycferry.connexionz.net/rtt/public/resource/gtfs.zip',
        },
        'gtfs_merge': 'prefixed',
        # MTA bus feeds share stop and route ids: one prefix, so shared stops/routes merge
        'gtfs_groups': {f: 'bus' for f in ('bus_m', 'bus_bx', 'bus_b', 'bus_q', 'bus_co')},
        'gtfs_extend_calendar': {'path'},  # PATH feed expired 2026-06; no newer one published
        'exact_service_match': True,
        'vehicle_classify': _route_type_only,
        'has_frequencies': False,
        # Geofabrik regions (make_osm_extract.py); the frame's left edge touches Jersey City/Bayonne
        'osm_pbf': ['north-america/us/new-york', 'north-america/us/new-jersey'],
        'network_tiles': 9,  # 7 before the 2026-10-10 bbox growth (tiles ~same size)
        'template': 'krakow',  # OSM layer stack (Warsaw's uses its old roads.shp); page follows render_frame
    },

    'lublin': {
        'name': 'Lublin',
        'bbox': {
            'south': 51.130, 'west': 22.430,
            'north': 51.310, 'east': 22.700,
        },
        'crs_metric': 'EPSG:2180',
        'gtfs': {
            'ztm': 'https://mkuran.pl/gtfs/lublin.zip',              # ZDiTM buses + trolleybuses
            'trains': 'https://mkuran.pl/gtfs/polish_trains.zip',    # clipped to the bbox
        },
        'gtfs_merge': 'prefixed',
        'exact_service_match': True,
        'vehicle_classify': _route_type_only,
        'has_frequencies': False,
        'template': 'krakow',
        # EPSG:2180 m: 85 % of the bbox, centred between it and the coverage's centre of mass
        'render_frame': (740050, 370830, 756850, 388630),
    },

    'amsterdam': {
        'name': 'Amsterdam',
        # City core (2026-10-10, smaller than the municipality 52.278-52.431 N, 4.729-5.108 E):
        # ring + Noord, Nieuw-West, Zuidoost, IJburg, Schiphol at the edge; no Weesp/polders
        'bbox': {
            'south': 52.290, 'west': 4.755,
            'north': 52.425, 'east': 5.030,
        },
        'osm_pbf': ['europe/netherlands/noord-holland'],  # Geofabrik, see make_osm_extract.py
        'crs_metric': 'EPSG:28992',  # RD New (metres)
        'gtfs': {
            # OVapi: all of the Netherlands (NS, GVB, Connexxion, EBS, ...), clipped to the bbox
            'nl': 'http://gtfs.ovapi.nl/nl/gtfs-nl.zip',
        },
        'gtfs_merge': 'prefixed',  # one feed, but its bbox clip + chunked reading are needed
        'exact_service_match': True,
        'vehicle_classify': _route_type_only,
        'has_frequencies': False,
        'network_tiles': 6,
        'template': 'krakow',
        # EPSG:28992 m: the bbox minus 400 m (17.8 x 14.1 km, landscape like Kraków's layout)
        'render_frame': (112400, 478590, 130230, 492680),
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
    city['osm_dir'] = Path("D:/QGIS/osm_basemap") / key
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
