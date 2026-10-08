"""
Create Coverage Map from Overlapping Transit Isochrones
Planar subdivision approach -- counts deduplicated transit frequency per area.

For each polygon piece created by overlapping isochrones:
- Collects route_trip_counts from all covering isochrones
- For each unique route, takes the max trip count (best stop for that line)
- Sums those maxes = deduplicated trip count

This prevents inflating frequency when the same bus line passes multiple
nearby stops that all fall within walking distance.

The subdivision is done per tile (STRtree picks the isochrones touching each
tile) in parallel worker processes. Pieces are snapped to a 1 cm grid so
tile edges line up and the final dissolve joins them seamlessly.

Incremental: tiles sit on a fixed grid and each finished tile is cached under a
fingerprint of the isochrones touching it (geometry + trip counts), in
<data>/.coverage_cache_<variant>/. A rerun after a small change (a few gates,
a few stops) only recomputes the tiles whose isochrones changed; an interrupted
run resumes for free. Fixed geometries (step 1) are cached the same way.

Usage:
    python 04_create_coverage_map.py --city warsaw [data_folder]
    python 04_create_coverage_map.py --city poznan
    python 04_create_coverage_map.py --city warsaw --barriers   # isochrones_barriers -> coverage_map_barriers
    python 04_create_coverage_map.py --city krakow --variant gates_residents
    python 04_create_coverage_map.py --city warsaw --workers 4 --tile 1500
"""
import argparse
import hashlib
import os
import pickle
import shutil
import geopandas as gpd
import numpy as np
import psutil
import shapely
from multiprocessing import Pool
from pathlib import Path
from datetime import datetime
from shapely.ops import unary_union, polygonize
from shapely.geometry import box
from shapely.strtree import STRtree
from shapely.validation import make_valid
import warnings
warnings.filterwarnings('ignore')

from cities import get_city, add_city_argument

TILE_M = 1500          # tile edge length in metres
SNAP_M = 0.01          # precision grid; makes both sides of a tile edge identical
DEFAULT_WORKERS = max(1, min(6, (os.cpu_count() or 2) - 2))  # RAM guard (MIN_FREE_GB) throttles if memory runs low
MIN_FREE_GB = 3.0      # pause dispatching new tiles below this much available RAM


def free_gb():
    return psutil.virtual_memory().available / 1024 ** 3


def fix_geometry(geom):
    """Fix a geometry using make_valid + buffer(0)."""
    if geom is None or geom.is_empty:
        return None
    try:
        geom = make_valid(geom)
        geom = geom.buffer(0)
        geom = geom.buffer(0.1).buffer(-0.1)
        geom = make_valid(geom)
        if geom.is_empty or geom.area < 10:
            return None
        return geom
    except Exception:
        return None


def parse_route_trip_counts(rtc_str):
    """Parse 'route:count,route:count,...' into dict {route_id: trip_count}."""
    result = {}
    if not rtc_str or rtc_str != rtc_str:  # handles NaN
        return result
    for pair in str(rtc_str).split(','):
        if ':' in pair:
            route, count = pair.rsplit(':', 1)
            try:
                result[route] = int(count)
            except ValueError:
                pass
    return result


def _cached(cache_dir, name, compute):
    """Return pickled result from cache_dir/name.pkl, computing and storing it if missing."""
    if cache_dir is None:
        return compute()
    f = cache_dir / f"{name}.pkl"
    if f.exists():
        print(f"  (loaded {name} from cache)")
        with open(f, 'rb') as fh:
            return pickle.load(fh)
    result = compute()
    with open(f, 'wb') as fh:
        pickle.dump(result, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return result


def _rings(geom):
    polys = geom.geoms if geom.geom_type in ('MultiPolygon', 'GeometryCollection') else [geom]
    out = []
    for p in polys:
        if p.geom_type == 'Polygon':
            out.append(p.exterior)
            out.extend(p.interiors)
    return out


def process_tile(task):
    """
    Planar subdivision of one tile: clip the isochrones touching it, union their
    boundaries, polygonize, and dedup routes per piece. Runs in a worker process.
    task = (tile_id, (minx, miny, maxx, maxy), [geometry], [route_trip_counts])
    Returns (tile_id, [(piece, unique_routes, deduped_trips), ...]).
    """
    tile_id, bounds, geoms, rtcs = task
    tile = box(*bounds)
    clipped = []
    for g, rtc in zip(geoms, rtcs):
        c = g.intersection(tile)
        if not c.is_empty and c.area > 0:
            clipped.append((c, rtc))
    if not clipped:
        return tile_id, []

    rings = [r for c, _ in clipped for r in _rings(c)]
    # Fixed-precision noding (snap to SNAP_M): plain floating-point noding can miss
    # crossings between near-collinear edges, so faces fail to close and area vanishes
    pieces = list(polygonize(shapely.union_all(rings, grid_size=SNAP_M)))
    if not pieces:
        return tile_id, []

    # Which clipped isochrones contain each piece
    tree = STRtree([c for c, _ in clipped])
    points = [p.representative_point() for p in pieces]
    piece_idx, iso_idx = tree.query(points, predicate='within')

    covering = {}
    for p, i in zip(piece_idx, iso_idx):
        covering.setdefault(p, []).append(clipped[i][1])

    out = []
    for p, rtc_list in covering.items():
        best = {}
        for rtc in rtc_list:
            for route, count in parse_route_trip_counts(rtc).items():
                if count > best.get(route, -1):
                    best[route] = count
        if best:
            out.append((pieces[p], len(best), sum(best.values())))

    # Pre-dissolve inside the tile: one shape per trip value. Pieces from one
    # polygonize form a clean coverage, so coverage_union_all is safe and fast.
    groups = {}
    for geom, routes, trips in out:
        groups.setdefault(trips, [routes, []])[1].append(geom)
    # Snapped here (in the worker, cached with the tile) so tile edges line up exactly
    merged = [(shapely.set_precision(_coverage_union(geoms), SNAP_M), routes, trips)
              for trips, (routes, geoms) in groups.items()]
    return tile_id, merged


def _coverage_union(geoms):
    if len(geoms) == 1:
        return geoms[0]
    try:
        merged = shapely.coverage_union_all(geoms)
        # Fast path assumes a perfectly noded coverage; if the result is invalid it can
        # silently corrupt the later cross-tile union, so fall back to the robust union
        if merged.is_valid:
            return merged
    except Exception:
        pass
    return unary_union(geoms)


def dissolve_group(task):
    """Final cross-tile dissolve of one trip value (runs in a worker)."""
    trips, routes, geoms = task
    geoms = [g if g.is_valid else make_valid(g) for g in geoms]  # one bad piece corrupts the union
    merged = unary_union(geoms)
    # Cleanup here, in parallel (was a slow single-process pass over all parts)
    if not merged.is_empty:
        merged = make_valid(merged.buffer(0))
    return trips, routes, merged


def _digest(*parts):
    h = hashlib.blake2b(digest_size=16)
    for p in parts:
        h.update(p if isinstance(p, bytes) else str(p).encode())
        h.update(b'|')
    return h.digest()


def make_tasks(gdf, tile_m):
    """
    Tiles on a fixed grid (multiples of tile_m, so they don't shift when the extent
    changes); STRtree selects the isochrones touching each tile. The task id is a
    fingerprint of the tile and its isochrones, used as the cache key.
    """
    minx, miny, maxx, maxy = gdf.total_bounds
    geoms = gdf.geometry.to_numpy()
    rtcs = gdf['route_trip_counts'].to_numpy()
    iso_keys = [_digest(shapely.to_wkb(g), r) for g, r in zip(geoms, rtcs)]
    tree = STRtree(geoms)
    tasks = []
    for x0 in np.arange(np.floor(minx / tile_m) * tile_m, maxx, tile_m):
        for y0 in np.arange(np.floor(miny / tile_m) * tile_m, maxy, tile_m):
            bounds = (float(x0), float(y0), float(x0 + tile_m), float(y0 + tile_m))
            hits = tree.query(box(*bounds))
            if len(hits):
                key = _digest(bounds, SNAP_M, *sorted(iso_keys[h] for h in hits)).hex()
                tasks.append((key, bounds, list(geoms[hits]), list(rtcs[hits])))
    return tasks


def create_coverage_map(input_file, crs_metric: str, cache_dir=None,
                        workers=DEFAULT_WORKERS, tile_m=TILE_M):
    """
    Planar subdivision:
    1. Fix all geometries
    2. Per tile: union clipped boundaries, polygonize, dedup routes per piece
    3. Dissolve adjacent pieces with the same deduped trip count

    Loads the isochrones itself and releases each stage once the next exists,
    so peak memory stays low on large cities.
    """
    t0 = datetime.now()
    stamp = lambda: f"[{(datetime.now() - t0).total_seconds():.0f}s]"
    print("Loading isochrones...", end=' ', flush=True)
    gdf = gpd.read_file(input_file)
    original_crs = gdf.crs
    gdf = gdf.to_crs(crs_metric)
    print(f"{len(gdf)} loaded")
    print(f"\nCreating coverage map from {len(gdf)} isochrones...\n")

    # Step 1: Fix geometries
    print("Step 1: Fixing geometries...")

    # Cached per input geometry, so unchanged isochrones are never re-fixed
    fixed_file = cache_dir / 'fixed.pkl' if cache_dir else None
    old = {}
    if fixed_file and fixed_file.exists():
        with open(fixed_file, 'rb') as fh:
            old = pickle.load(fh)
    new, out_geoms, n_new = {}, [], 0
    for idx, geom in enumerate(gdf.geometry):
        k = _digest(shapely.to_wkb(geom))
        if k in old:
            wkb = old[k]
        else:
            fixed = fix_geometry(geom)
            wkb = shapely.to_wkb(fixed) if fixed is not None else None
            n_new += 1
            if n_new % 1000 == 0:
                print(f"    {n_new} fixed...", flush=True)
        new[k] = wkb
        out_geoms.append(wkb)
    if fixed_file:
        with open(fixed_file, 'wb') as fh:
            pickle.dump(new, fh, protocol=pickle.HIGHEST_PROTOCOL)
    del old, new
    valid = np.array([w is not None for w in out_geoms])
    gdf = gdf[valid].copy()
    gdf['geometry'] = shapely.from_wkb([w for w in out_geoms if w is not None])
    del out_geoms
    print(f"  {stamp()} {len(gdf)} geometries validated ({n_new} new, {int(valid.size) - n_new} cached)\n")

    # Step 2: Tiled planar subdivision + route dedup
    tasks = make_tasks(gdf, tile_m)
    del gdf  # tasks hold the geometries they need
    tiles_dir = cache_dir / f"tiles_{tile_m}_v4" if cache_dir else None  # v4: fingerprint keys, snapped
    if tiles_dir:
        tiles_dir.mkdir(exist_ok=True)
    done = {}
    if tiles_dir:
        for t in tasks:
            f = tiles_dir / f"{t[0]}.pkl"
            if f.exists():
                with open(f, 'rb') as fh:
                    done[t[0]] = pickle.load(fh)
    todo = [t for t in tasks if t[0] not in done]
    print(f"Step 2: Subdividing {len(tasks)} tiles of {tile_m} m "
          f"({len(done)} cached, {len(todo)} to do, {workers} workers)...", flush=True)

    start = datetime.now()
    report_every = max(1, len(todo) // 40)
    n = 0
    queue = list(reversed(todo))
    in_flight = []
    with Pool(workers) as pool:
        while queue or in_flight:
            # Dispatch while there is a free worker slot and enough free RAM
            while queue and len(in_flight) < workers:
                if in_flight and free_gb() < MIN_FREE_GB:
                    break  # let running tiles finish first
                in_flight.append(pool.apply_async(process_tile, (queue.pop(),)))
            # Collect whatever has finished
            ready = [r for r in in_flight if r.ready()]
            if not ready:
                in_flight[0].wait(0.5)
                continue
            for r in ready:
                in_flight.remove(r)
                tile_id, result = r.get()
                done[tile_id] = result
                if tiles_dir:
                    with open(tiles_dir / f"{tile_id}.pkl", 'wb') as fh:
                        pickle.dump(result, fh, protocol=pickle.HIGHEST_PROTOCOL)
                n += 1
                if n % report_every == 0 or n == len(todo):
                    el = (datetime.now() - start).total_seconds()
                    print(f"    tile {n}/{len(todo)} — {el:.0f}s elapsed, "
                          f"~{el / n * (len(todo) - n):.0f}s remaining, "
                          f"{free_gb():.1f} GB RAM free", flush=True)

    if tiles_dir:  # keep only the tiles of this run
        for f in tiles_dir.glob('*.pkl'):
            if f.stem not in done:
                f.unlink()
    del tasks, todo, queue, in_flight
    rows = [r for result in done.values() for r in result]
    del done
    if not rows:
        print("ERROR: No polygons created")
        return None
    pieces_gdf = gpd.GeoDataFrame(
        {'unique_routes': [r[1] for r in rows], 'deduped_trips': [r[2] for r in rows]},
        geometry=[r[0] for r in rows],  # already snapped per tile
        crs=crs_metric)
    del rows
    pieces_gdf = pieces_gdf[~pieces_gdf.geometry.is_empty]
    print(f"  {stamp()} {len(pieces_gdf)} pieces with coverage\n")

    # Step 3: Dissolve across tile edges, one trip value per task, in parallel
    groups = {}
    for routes, trips, geom in zip(pieces_gdf['unique_routes'], pieces_gdf['deduped_trips'],
                                   pieces_gdf.geometry):
        groups.setdefault(trips, [routes, []])[1].append(geom)
    tasks3 = sorted(((t, r, g) for t, (r, g) in groups.items()), key=lambda x: -len(x[2]))
    del pieces_gdf, groups
    print(f"Step 3: Dissolving {len(tasks3)} trip values across tiles "
          f"({workers} workers)...", flush=True)
    start = datetime.now()
    out3 = []
    report_every = max(1, len(tasks3) // 10)
    with Pool(workers) as pool:
        for n, res in enumerate(pool.imap_unordered(dissolve_group, tasks3), 1):
            out3.append(res)
            if n % report_every == 0 or n == len(tasks3):
                el = (datetime.now() - start).total_seconds()
                print(f"    {n}/{len(tasks3)} values — {el:.0f}s elapsed, "
                      f"{free_gb():.1f} GB RAM free", flush=True)
    del tasks3
    dissolved = gpd.GeoDataFrame(
        {'deduped_trips': [o[0] for o in out3], 'unique_routes': [o[1] for o in out3]},
        geometry=[o[2] for o in out3], crs=crs_metric)
    del out3
    dissolved = dissolved.explode(index_parts=False).reset_index(drop=True)
    print(f"  {stamp()} {len(dissolved)} polygons after dissolve\n")

    # Step 4: Cleanup
    print("Step 4: Cleanup...")
    dissolved = dissolved[dissolved.geometry.notna()]
    dissolved = dissolved[dissolved.geom_type.isin(['Polygon', 'MultiPolygon'])]
    dissolved = dissolved[~dissolved.geometry.is_empty]
    dissolved = dissolved[dissolved.geometry.is_valid]
    dissolved = dissolved[dissolved.geometry.area > 10]
    dissolved = dissolved[dissolved.geometry.area <= 5_000_000]

    dissolved['area_ha'] = dissolved.geometry.area / 10000
    dissolved = dissolved[['deduped_trips', 'unique_routes', 'area_ha', 'geometry']].copy()
    dissolved = dissolved.to_crs(original_crs)
    dissolved = dissolved.sort_values('deduped_trips', ascending=False).reset_index(drop=True)

    print(f"  {stamp()} Final: {len(dissolved)} polygons\n")
    return dissolved


def find_latest_data_dir(city: dict, suffix: str = "") -> Path:
    """Find the most recent data folder with isochrones."""
    base = city['data_dir']
    if not base.exists():
        raise FileNotFoundError(f"No data directory for {city['name']}: {base}")
    data_dirs = sorted(
        [d for d in base.iterdir() if d.is_dir() and (d / f'isochrones{suffix}.gpkg').exists()],
        key=lambda x: x.name, reverse=True
    )
    if not data_dirs:
        raise FileNotFoundError(
            f"No isochrones.gpkg in {base}. Run 03_generate_isochrones_local.py first."
        )
    return data_dirs[0]


def main():
    parser = argparse.ArgumentParser(description='Create coverage map')
    add_city_argument(parser)
    parser.add_argument('--barriers', action='store_true',
                        help='Use barrier-aware isochrones (same as --variant barriers)')
    parser.add_argument('--variant', default=None,
                        help='Isochrone variant: reads isochrones_<variant>.gpkg, writes '
                             'coverage_map_<variant>.gpkg (e.g. gates, gates_residents)')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                        help=f'Parallel worker processes (default {DEFAULT_WORKERS})')
    parser.add_argument('--tile', type=int, default=TILE_M, help=f'Tile size in m (default {TILE_M})')
    parser.add_argument('data_folder', nargs='?', help='Data folder (default: most recent)')
    args = parser.parse_args()
    _rt = Path(__file__).resolve().parent / 'runtime.json'  # machine-level overrides
    if _rt.exists():
        import json
        _cfg = json.loads(_rt.read_text(encoding='utf-8'))
        args.workers = max(args.workers, int(_cfg.get('min_workers', 0)))
        if 'min_free_gb' in _cfg:
            globals()['MIN_FREE_GB'] = float(_cfg['min_free_gb'])
        print(f"runtime.json: workers={args.workers}, min_free_gb={_cfg.get('min_free_gb', '-')}")

    city = get_city(args.city)
    crs_metric = city['crs_metric']
    variant = args.variant or ("barriers" if args.barriers else "")
    suffix = f"_{variant}" if variant else ""

    data_dir = Path(args.data_folder) if args.data_folder else find_latest_data_dir(city, suffix)
    input_file = data_dir / f"isochrones{suffix}.gpkg"
    output_file = data_dir / f"coverage_map{suffix}.gpkg"

    print("=" * 60)
    print(f"Transit Coverage Map — {city['name']}")
    print("Deduplicated frequency per area")
    print("=" * 60)
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}\n")

    if not input_file.exists():
        print(f"Not found: {input_file}")
        print("Run 03_generate_isochrones_local.py first.")
        return

    # Persistent incremental cache (see module docstring)
    cache_dir = data_dir / f".coverage_cache{suffix}"
    cache_dir.mkdir(exist_ok=True)
    import re
    for old_dir in data_dir.glob(f".cache_coverage{suffix}_*"):  # pre-incremental caches
        if re.fullmatch(rf"\.cache_coverage{suffix}_\d+_\d+", old_dir.name):
            shutil.rmtree(old_dir, ignore_errors=True)

    start_time = datetime.now()
    coverage = create_coverage_map(input_file, crs_metric, cache_dir, args.workers, args.tile)

    if coverage is None or len(coverage) == 0:
        print("Failed to create coverage map!")
        return

    print("Saving...", end=' ', flush=True)
    coverage.to_file(output_file, driver="GPKG")
    elapsed = (datetime.now() - start_time).total_seconds()
    print("Done")

    print(f"\n{'='*60}")
    print(f"Coverage map created in {elapsed/60:.1f} min")
    print(f"{'='*60}")
    print(f"\n  Polygons: {len(coverage):,}")
    print(f"  Max deduped trips: {coverage['deduped_trips'].max()}")
    print(f"  Max unique routes: {coverage['unique_routes'].max()}")
    print(f"  Total coverage: {coverage['area_ha'].sum():.0f} ha")

    print(f"\nTop 15 by deduped trips:")
    for n in sorted(coverage['deduped_trips'].unique(), reverse=True)[:15]:
        sub = coverage[coverage['deduped_trips'] == n]
        routes = sub['unique_routes'].iloc[0]
        print(f"  {n:>5} trips ({routes:>3} routes): {len(sub):>5} polygons, {sub['area_ha'].sum():>7.1f} ha")

    print(f"\nSaved to: {output_file}")


if __name__ == '__main__':
    main()
