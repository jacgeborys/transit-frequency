"""
Coverage map on a raster grid: same result as 04_create_coverage_map.py, much faster.

Per pixel (default 2 m): for each route, the best trip count among the isochrones covering
the pixel (that route's best stop), summed over routes = deduplicated departures; the number
of routes = unique_routes. Per route, its stops' isochrones are burnt into a grid in
ascending trip order, so the last write is the per-pixel maximum; that grid is added to the
total. The total is polygonized by value into the same GeoPackage schema as the vector
method (deduped_trips, unique_routes, area_ha), so 05 and the renderer read it unchanged.

Differences to the vector method: edges follow the pixel grid (2 m steps), and pieces under
one pixel vanish. unique_routes is the highest route count among the pixels of a trip
value (the vector method keeps the first piece's).

Usage:
    python 04_coverage_raster.py --city lublin --variant gates [data_folder]
        -> coverage_map_gates_raster.gpkg (--out to choose another file)
"""
import argparse
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio.features
import shapely
from rasterio.transform import from_origin
from shapely.validation import make_valid

from cities import get_city, add_city_argument

RES_M = 2.0


def parse_rtc(s):
    out = {}
    if isinstance(s, str):
        for pair in s.split(','):
            if ':' in pair:
                route, count = pair.rsplit(':', 1)
                try:
                    out[route] = int(count)
                except ValueError:
                    pass
    return out


def _lattice_origin(arg, res):
    """Grid origin <= lo whose cell edges lie on the vertices' dominant lattice (mod res)."""
    lo, coords = arg
    m = np.round(np.mod(coords, res), 3) % res
    u, n = np.unique(m, return_counts=True)
    off = float(u[n.argmax()]) if len(n) and n.max() > 0.5 * len(m) else res / 2  # no lattice: any
    return np.floor((lo - off) / res) * res + off


def coverage_raster(iso, res=RES_M, log=print):
    """Isochrones (metric CRS, column route_trip_counts) -> (trips int32, routes int16, transform)."""
    t0 = time.time()
    geoms = [make_valid(g) if not g.is_valid else g for g in iso.geometry]
    minx, miny, maxx, maxy = iso.total_bounds
    # 03's barrier isochrones have their vertices on a 2 m lattice (offset differs per city:
    # Lublin odd/odd, Kraków even/odd metres). Cell centres on that lattice would sit exactly on
    # edges (ties either way), so cell edges go on the lattice and centres fall in between.
    minx, miny = (_lattice_origin(v, res) for v in (
        (minx, iso.geometry.get_coordinates()['x'].to_numpy()[:200000]),
        (miny, iso.geometry.get_coordinates()['y'].to_numpy()[:200000])))
    w = int(np.ceil((maxx - minx) / res)) + 1
    h = int(np.ceil((maxy - miny) / res)) + 1
    transform = from_origin(minx, miny + h * res, res, res)
    log(f"  grid {w} x {h} px at {res} m ({w * h / 1e6:.0f} M px)")

    # route -> [(trip count, isochrone index)]
    by_route = {}
    for i, rtc in enumerate(iso['route_trip_counts']):
        for route, count in parse_rtc(rtc).items():
            if count > 0:
                by_route.setdefault(route, []).append((count, i))
    bounds = shapely.bounds(np.array(geoms, dtype=object))

    trips = np.zeros((h, w), dtype=np.int32)
    routes = np.zeros((h, w), dtype=np.int16)
    for n, (route, items) in enumerate(sorted(by_route.items()), 1):
        idx = [i for _, i in items]
        # Window = bounding box of this route's isochrones, in pixels
        x0, y0 = bounds[idx, 0].min(), bounds[idx, 1].min()
        x1, y1 = bounds[idx, 2].max(), bounds[idx, 3].max()
        c0 = max(0, int((x0 - minx) / res) - 1)
        c1 = min(w, int(np.ceil((x1 - minx) / res)) + 1)
        r0 = max(0, int((miny + h * res - y1) / res) - 1)
        r1 = min(h, int(np.ceil((miny + h * res - y0) / res)) + 1)
        win_t = from_origin(minx + c0 * res, miny + h * res - r0 * res, res, res)
        shapes = [(geoms[i], c) for c, i in sorted(items)]   # ascending: last write = max
        best = rasterio.features.rasterize(shapes, out_shape=(r1 - r0, c1 - c0), transform=win_t,
                                           fill=0, dtype='int32')
        trips[r0:r1, c0:c1] += best
        routes[r0:r1, c0:c1] += (best > 0)
        if n % max(1, len(by_route) // 10) == 0:
            log(f"    {n}/{len(by_route)} routes, {time.time() - t0:.0f}s")
    log(f"  [{time.time() - t0:.0f}s] {len(by_route)} routes burnt in")
    return trips, routes, transform


def polygonize(trips, routes, transform, crs, log=print):
    """Raster -> GeoDataFrame like 04_create_coverage_map.py's output."""
    t0 = time.time()
    # Highest route count per trip value (the vector method's per-value routes figure)
    vals = trips.ravel()
    sel = vals > 0
    max_routes = {}
    if sel.any():
        u, inv = np.unique(vals[sel], return_inverse=True)
        mr = np.zeros(len(u), dtype=np.int32)
        np.maximum.at(mr, inv, routes.ravel()[sel].astype(np.int32))
        max_routes = dict(zip(u.tolist(), mr.tolist()))
    geoms, values = [], []
    for geom, v in rasterio.features.shapes(trips, mask=trips > 0, transform=transform,
                                            connectivity=4):
        geoms.append(shapely.geometry.shape(geom))
        values.append(int(v))
    gdf = gpd.GeoDataFrame({'deduped_trips': values}, geometry=geoms, crs=crs)
    gdf = gdf.dissolve(by='deduped_trips', as_index=False)   # one (multi)polygon per value
    gdf = gdf.explode(index_parts=False).reset_index(drop=True)
    gdf = gdf[(gdf.geometry.area > 10) & (gdf.geometry.area <= 5_000_000)]  # as the vector method
    gdf['unique_routes'] = gdf['deduped_trips'].map(max_routes).astype(int)
    gdf['area_ha'] = gdf.geometry.area / 10000
    gdf = gdf[['deduped_trips', 'unique_routes', 'area_ha', 'geometry']]
    log(f"  [{time.time() - t0:.0f}s] {len(gdf):,} polygons")
    return gdf.sort_values('deduped_trips', ascending=False).reset_index(drop=True)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    ap.add_argument('--variant', default='gates', help='isochrones_<variant>.gpkg (e.g. gates, gates_residents)')
    ap.add_argument('--res', type=float, default=RES_M, help=f'Pixel size in m (default {RES_M})')
    ap.add_argument('--out', help='Output GeoPackage (default coverage_map_<variant>_raster.gpkg)')
    ap.add_argument('data_folder', nargs='?')
    args = ap.parse_args()
    city = get_city(args.city)
    data = Path(args.data_folder) if args.data_folder else sorted(
        d for d in city['data_dir'].iterdir() if (d / f'isochrones_{args.variant}.gpkg').exists())[-1]
    suffix = f"_{args.variant}" if args.variant else ''
    out = Path(args.out) if args.out else data / f"coverage_map{suffix}_raster.gpkg"
    t0 = time.time()
    iso = gpd.read_file(data / f"isochrones{suffix}.gpkg")
    crs = iso.crs
    iso = iso.to_crs(city['crs_metric'])
    print(f"{city['name']} {args.variant}: {len(iso):,} isochrones -> {out.name}")
    trips, routes, transform = coverage_raster(iso, args.res)
    gdf = polygonize(trips, routes, transform, city['crs_metric']).to_crs(crs)
    gdf.to_file(out, driver='GPKG')
    print(f"Done in {(time.time() - t0) / 60:.1f} min: {len(gdf):,} polygons, "
          f"{gdf['area_ha'].sum():.0f} ha, max {gdf['deduped_trips'].max()} trips -> {out}")


if __name__ == '__main__':
    main()
