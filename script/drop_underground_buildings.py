"""
Remove underground structures from a city's OSM buildings layer.

OSM maps some underground structures as buildings tagged location=underground: subway
stations (New York's Times Sq-42 St, Grand Central's platforms), Warszawa Centralna's
platform hall, substations, bunkers. Drawn on the map they lie across streets, and the
access model treats them as buildings you can enter but not cross. The basemap fetcher
didn't keep the location tag before 2026-10-09, so this asks Overpass for those ids and
drops them from buildings.gpkg (original kept as buildings_with_underground.gpkg).

Usage:
    python drop_underground_buildings.py --city manhattan
Then rerun 03-05 (or only 05 + render if the structures are small).
"""
import argparse
import shutil

import geopandas as gpd

import overpass_polite
from cities import get_city, add_city_argument

HEADERS = {'User-Agent': 'QGIS-basemap-fetcher/1.0', 'Accept': '*/*'}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    args = ap.parse_args()
    city = get_city(args.city)
    b = city['bbox']
    bb = f"{b['south']},{b['west']},{b['north']},{b['east']}"
    q = (f'[out:json][timeout:120];(way["building"]["location"="underground"]({bb});'
         f'relation["building"]["location"="underground"]({bb}););out tags;')
    data = overpass_polite.post(q, HEADERS, timeout=180)
    print()
    if data is None:
        raise SystemExit("Overpass failed; nothing changed. Try again later.")
    ids = {e['id'] for e in data['elements']}
    names = sorted({e['tags'].get('name', '') for e in data['elements']} - {''})
    print(f"{len(ids)} underground buildings in OSM: {', '.join(names[:10])}{' ...' if len(names) > 10 else ''}")

    f = city['osm_dir'] / 'buildings.gpkg'
    backup = city['osm_dir'] / 'buildings_with_underground.gpkg'
    if not backup.exists():
        shutil.copy(f, backup)
    bld = gpd.read_file(backup)
    drop = bld['osm_id'].astype('int64').isin(ids)
    print(f"{drop.sum()} of {len(bld):,} footprints removed -> {f}")
    bld[~drop].to_file(f, driver='GPKG')


if __name__ == '__main__':
    main()
