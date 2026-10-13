"""
Add fences/walls tagged on multipolygon relations to a city's existing barriers.gpkg.

Until 2026-10-13 fetch_osm_basemap.py fetched barriers from ways only, so a fence mapped on a
relation (e.g. ROD "Małe Błonia" in Kraków: barrier=fence + landuse=allotments on the
multipolygon) was missing from the walk model. This adds just those relations (one query for
the whole bbox, via the local Geofabrik extract when the city has one) without refetching the
layer. The old file is kept as barriers_ways_only.gpkg.

Takes effect in the maps only after 03 is rerun (run_city --from iso); sketch_access.py
--recompute uses it right away.

Usage:
    python add_barrier_relations.py --city krakow
"""
import argparse
import importlib.util
import os
import shutil

import geopandas as gpd
import pandas as pd

from cities import get_city, add_city_argument

FETCHER = r'D:\QGIS\osm_basemap\fetch_osm_basemap.py'
PBF_DIR = r'D:\QGIS\osm_basemap\pbf'
QUERY = """
[out:json][timeout:180];
(
  relation["barrier"~"^(fence|wall|city_wall|retaining_wall|hedge)$"]({bbox});
);
out geom;
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    args = ap.parse_args()
    city = get_city(args.city)

    regions = [r.split('/')[-1] for r in city.get('osm_pbf', [])]
    if regions:  # answer from the city's local extract, as run_city does
        os.environ['OSM_LOCAL_PBF'] = ';'.join(
            os.path.join(PBF_DIR, f"{city['key']}_{r}.osm.pbf") for r in regions)
    spec = importlib.util.spec_from_file_location('fetcher', FETCHER)
    fetcher = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fetcher)

    b = city['bbox']
    bbox = f"{b['south']},{b['west']},{b['north']},{b['east']}"
    rel = fetcher.query_overpass(QUERY.replace('{bbox}', bbox), 'barriers',
                                 filter_columns=fetcher.COLUMN_FILTERS['barriers'])
    if rel is None:
        raise SystemExit('Query failed')
    path = city['osm_dir'] / 'barriers.gpkg'
    backup = city['osm_dir'] / 'barriers_ways_only.gpkg'
    old = gpd.read_file(path)
    old = old[old['osm_type'] != 'relation']  # rerunnable: replace relations added earlier
    print(f"{len(rel)} barrier relations in the bbox"
          + (': ' + ', '.join(sorted(rel['barrier'].astype(str).unique())) if len(rel) else ''))
    if not backup.exists():
        shutil.copy2(path, backup)
    new = gpd.GeoDataFrame(pd.concat([old, rel.to_crs(old.crs)], ignore_index=True), crs=old.crs)
    fetcher.write_gpkg(new, path)
    print(f"Saved {path}: {len(old):,} ways + {len(rel)} relations (backup {backup.name})")


if __name__ == '__main__':
    main()
