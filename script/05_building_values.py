"""
Assign each building the best transit frequency reachable from it.

For every building footprint, takes the MAX deduped_trips of the coverage map
cells it touches ("leave by the best side"). Buildings larger than
BIG_BUILDING_M2 (malls, stations, halls) are flagged `big` and not coloured by
the renderer: their mapped indoor corridors would otherwise dominate.

With a residents coverage map (gate mode), buildings reached only through
closed gates get their value from it and are flagged `residents_only`.

Usage:
    python 05_building_values.py --city krakow --variant gates [data_folder]
    -> buildings_gates.gpkg in the data folder
       (columns: osm_id, max_trips, residents_only, area_m2, big)
"""
import argparse
from pathlib import Path

import geopandas as gpd
import numpy as np
from rasterio.features import rasterize
from rasterio.transform import from_origin

from cities import get_city, add_city_argument

CELL_M = 3.0
STRIP_M = 3000          # rasterize in 3 km strips to keep RAM low
BIG_BUILDING_M2 = 5000
EXCLUDED_BUILDING_TYPES = {'roof', 'carport', 'service'}  # as in the QGIS buildings layer


def max_per_building(buildings, coverage, cell=CELL_M, strip_m=STRIP_M):
    """
    MAX coverage deduped_trips touching each building (0 = not covered).
    Rasterizes in horizontal strips (spatial index per strip) to keep RAM low;
    a building crossing a strip edge gets the max over both strips.
    """
    from shapely.geometry import box
    x0, y0, x1, y1 = coverage.total_bounds
    x0, y0, x1, y1 = x0 - cell, y0 - cell, x1 + cell, y1 + cell
    w = int((x1 - x0) / cell) + 1
    cov_geoms, cov_vals = coverage.geometry.values, coverage['deduped_trips'].to_numpy('int32')
    b_geoms, b_ids = buildings.geometry.values, np.arange(1, len(buildings) + 1, dtype='int32')
    cov_idx, b_idx = coverage.sindex, buildings.sindex
    out = np.zeros(len(buildings) + 1, dtype='int32')
    for sy0 in np.arange(y0, y1, strip_m):
        h = int(np.ceil(min(strip_m, y1 - sy0) / cell))
        top = sy0 + h * cell
        strip = box(x0, sy0, x1, top)
        ci, bi = cov_idx.query(strip), b_idx.query(strip)
        if len(ci) == 0 or len(bi) == 0:
            continue
        transform = from_origin(x0, top, cell, cell)
        cov = rasterize(zip(cov_geoms[ci], cov_vals[ci]), out_shape=(h, w),
                        transform=transform, dtype='int32')
        bid = rasterize(zip(b_geoms[bi], b_ids[bi]), out_shape=(h, w),
                        transform=transform, dtype='int32', all_touched=True)
        mask = bid > 0
        np.maximum.at(out, bid[mask], cov[mask])
    return out[1:]


def main():
    parser = argparse.ArgumentParser(description='Best transit frequency per building')
    add_city_argument(parser)
    parser.add_argument('--variant', default='', help='coverage_map_<variant>.gpkg (e.g. gates)')
    parser.add_argument('data_folder', nargs='?')
    args = parser.parse_args()
    city = get_city(args.city)
    crs = city['crs_metric']
    suffix = f"_{args.variant}" if args.variant else ""

    data_dir = Path(args.data_folder) if args.data_folder else sorted(
        d for d in city['data_dir'].iterdir() if (d / f'coverage_map{suffix}.gpkg').exists())[-1]
    cov_file = data_dir / f'coverage_map{suffix}.gpkg'
    res_file = data_dir / f'coverage_map{suffix}_residents.gpkg'
    out_file = data_dir / f'buildings{suffix or "_base"}.gpkg'
    print(f"Coverage: {cov_file.name}" + (f" + {res_file.name}" if res_file.exists() else ""))

    coverage = gpd.read_file(cov_file).to_crs(crs)
    print("Loading buildings...", end=' ', flush=True)
    b = gpd.read_file(city['osm_dir'] / 'buildings.gpkg',
                      bbox=tuple(gpd.GeoSeries.from_xy(coverage.total_bounds[[0, 2]], coverage.total_bounds[[1, 3]], crs=crs)
                                 .to_crs(4326).total_bounds))
    if 'building' in b.columns:
        b = b[~b['building'].isin(EXCLUDED_BUILDING_TYPES)]
    b = b[b.geometry.notna() & b.geom_type.isin(['Polygon', 'MultiPolygon'])].to_crs(crs)
    b = b.reset_index(drop=True)
    print(f"{len(b):,}")

    print("Max frequency per building...", end=' ', flush=True)
    b['max_trips'] = max_per_building(b, coverage)
    b['residents_only'] = False
    if res_file.exists():
        res = max_per_building(b, gpd.read_file(res_file).to_crs(crs))
        only = (b['max_trips'] == 0) & (res > 0)
        b.loc[only, 'max_trips'] = res[only.to_numpy()]
        b.loc[only, 'residents_only'] = True
    print("done")

    b['area_m2'] = b.geometry.area.round(0)
    b['big'] = b['area_m2'] > BIG_BUILDING_M2
    b = b[(b['max_trips'] > 0) | b['big']]  # uncovered small buildings keep the basemap style
    keep = [c for c in ['osm_id', 'max_trips', 'residents_only', 'area_m2', 'big', 'geometry']
            if c in b.columns]
    b[keep].to_crs(4326).to_file(out_file, driver='GPKG')
    print(f"Saved {out_file.name}: {(b['max_trips'] > 0).sum():,} covered buildings "
          f"({b['residents_only'].sum():,} residents-only), {b['big'].sum():,} big (> {BIG_BUILDING_M2} m2)")


if __name__ == '__main__':
    main()
