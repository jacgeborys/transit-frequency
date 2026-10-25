"""
Per-building transit access split by mode (for the housing-price model in D:\\QGIS\\ceny_mieszkan).

For each building and walking time (5 and 10 min, isochrones from 03 --gates [--major-barriers]):
- dep<T>_<mode>: departures/day 06-22 by mode (metro, tram, bus, rail = SKM/KM/WKD; rail
  replacement buses count as bus). Each line *and direction* counts once, at its best stop
  inside the walk area (a trip calling at several stops counts once; the two directions of a
  line are different services). Departures only: a trip's last stop (arrival) and stops
  with pickup_type=1 don't count.
- lines<T>_<mode>: distinct lines by mode.
  Both are taken at the building's best 2 m cell (most departures, as max_trips in 05: "leave
  by the best side"), so the modes add up to dep<T>_all / lines<T>_all.
- line<T>_all: the old max_trips rule (each line once at its best stop, both directions
  together, all stop_times incl. arrivals) at its own best cell: comparable with 05's max_trips.
- night<T>_dep / night<T>_lines: night buses (N lines, 22-06), same rule as dep<T>.
- newtram<T>: opening date (YYYYMMDD) of the newest post-2015 tram section with a stop in
  the walk area (0 = none); stops and dates from --new-tram (CSV stop_id,opened,section).
- residents_only<T>: the building is reached only through closed gates / private ways (its
  values come from the residents isochrones, as in 05).
- walk_<mode>: walking time (min) to the nearest stop of each mode over the same network
  (gates, major roads/railways), capped at --cap (default 30 = not within 30 min).

Usage:
    python 06_building_modes.py --city warsaw --date 20261007 --variant gates_mb \\
        --new-tram warsaw_tram_openings.csv [data_folder]
    -> <data_folder>/buildings_modes.gpkg (+ buildings_modes_report.txt)
"""
import argparse
import importlib.util
import pickle
import time
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from rasterio.features import rasterize
from rasterio.transform import from_origin
from shapely.geometry import box

from cities import get_city, add_city_argument
from access_rules import closed_gate_ids, private_way_ids

SCRIPT_DIR = Path(__file__).resolve().parent
CELL_M = 2.0                 # as 04_coverage_raster
TILE_M = 3000
MODES = ['metro', 'tram', 'bus', 'rail']
EXCLUDED_BUILDING_TYPES = {'roof', 'carport', 'service'}   # as 05
BIG_BUILDING_M2 = 5000
WALK_M_PER_MIN = 4.5 * 1000 / 60


def _load(name):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / f'{name}.py')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def mode_of(route_id, vehicle):
    if str(route_id).endswith('_BUS'):          # KM rail replacement buses
        return 'bus'
    return {'train': 'rail'}.get(vehicle, vehicle if vehicle in MODES else 'bus')


def departures(city, data_dir, date):
    """Per-stop counts: (line rule, direction rule, night rule) DataFrames."""
    m01 = _load('01_calculate_trip_counts')
    day, _ = m01.load_departures(city, data_dir, date)
    day['mode'] = [mode_of(r, v) for r, v in zip(day['route_id'], day['vehicle'])]
    day['dir'] = pd.to_numeric(day['direction_id'], errors='coerce').fillna(-1).astype(int)
    line = day.groupby(['stop_id', 'route_id', 'mode']).size().rename('n').reset_index()
    board = day[~day.get('last_stop', False)]
    if 'pickup_type' in board.columns:
        board = board[pd.to_numeric(board['pickup_type'], errors='coerce').fillna(0) != 1]
    dirr = board.groupby(['stop_id', 'route_id', 'dir', 'mode']).size().rename('n').reset_index()
    night, _ = m01.load_departures(city, data_dir, date, night=True)
    night = night[night['route_id'].astype(str).str.match(r'^N\d')]
    night = night[~night.get('last_stop', False)]
    night['dir'] = pd.to_numeric(night['direction_id'], errors='coerce').fillna(-1).astype(int)
    nightr = night.groupby(['stop_id', 'route_id', 'dir']).size().rename('n').reset_index()
    return line, dirr, nightr


def key_groups(counts, iso_index, keycols):
    """[(key, mode, route, iso indices, counts ascending)] for the stops that have an isochrone."""
    c = counts[counts['stop_id'].isin(iso_index.index)].copy()
    c['iso'] = iso_index.loc[c['stop_id']].to_numpy()
    out = []
    for key, g in c.groupby(keycols, sort=False):
        g = g.sort_values('n')
        out.append((key, g['mode'].iloc[0] if 'mode' in g else None,
                    key[0] if isinstance(key, tuple) else key,
                    g['iso'].to_numpy(), g['n'].to_numpy('int32')))
    return out


def per_building(bld, iso, line, dirr, nightr, newtram, log):
    """
    One pass over the city in tiles. Returns a dict of per-building arrays (values at the
    building's best cell for the direction rule; own maxima for line/night/newtram).
    """
    iso_index = pd.Series(np.arange(len(iso)), index=iso['stop_id'].astype(str))
    iso_index = iso_index[~iso_index.index.duplicated()]
    g_line = key_groups(line, iso_index, ['route_id'])
    g_dir = key_groups(dirr, iso_index, ['route_id', 'dir'])
    nr = nightr.assign(mode='bus')
    g_night = key_groups(nr, iso_index, ['route_id', 'dir'])
    g_nightl = key_groups(nr.groupby(['stop_id', 'route_id', 'mode'])['n'].sum().reset_index(),
                          iso_index, ['route_id'])
    nt = newtram[newtram['stop_id'].isin(iso_index.index)]
    nt_iso = iso_index.loc[nt['stop_id']].to_numpy()
    nt_val = nt['opened'].to_numpy('int32')
    o = np.argsort(nt_val)
    nt_iso, nt_val = nt_iso[o], nt_val[o]

    geoms = iso.geometry.values
    sidx = iso.sindex
    n = len(bld)
    res = {f'dep_{m}': np.zeros(n, 'int32') for m in MODES + ['all']}
    res.update({f'lines_{m}': np.zeros(n, 'int16') for m in MODES + ['all']})
    res.update(line_all=np.zeros(n, 'int32'), night_dep=np.zeros(n, 'int32'),
               night_lines=np.zeros(n, 'int16'), newtram=np.zeros(n, 'int32'))
    best = np.full(n, -1, 'int64')
    bgeoms, bsidx = bld.geometry.values, bld.sindex
    bids = np.arange(1, n + 1, dtype='int32')

    x0, y0, x1, y1 = bld.total_bounds
    tiles = [(tx, ty) for ty in np.arange(y0, y1, TILE_M) for tx in np.arange(x0, x1, TILE_M)]
    t0 = time.time()
    for ti, (tx, ty) in enumerate(tiles, 1):
        tb = box(tx, ty, tx + TILE_M, ty + TILE_M)
        bi = bsidx.query(tb)
        if len(bi) == 0:
            continue
        # tile grid padded to cover its buildings completely
        bx0, by0, bx1, by1 = bld.geometry.iloc[bi].total_bounds
        gx0, gy1 = np.floor(min(bx0, tx) / CELL_M) * CELL_M, np.ceil(max(by1, ty + TILE_M) / CELL_M) * CELL_M
        gx1, gy0 = max(bx1, tx + TILE_M), min(by0, ty)
        w, h = int(np.ceil((gx1 - gx0) / CELL_M)) + 1, int(np.ceil((gy1 - gy0) / CELL_M)) + 1
        tr = from_origin(gx0, gy1, CELL_M, CELL_M)
        cand = np.zeros(len(iso), bool)
        cand[sidx.query(box(gx0, gy1 - h * CELL_M, gx0 + w * CELL_M, gy1))] = True

        def burn(groups):
            """Yield (group, per-cell max grid) for groups with an isochrone in this tile."""
            for grp in groups:
                idx, cnt = grp[3], grp[4]
                sel = cand[idx]
                if sel.any():
                    yield grp, rasterize(zip(geoms[idx[sel]], cnt[sel]), out_shape=(h, w),
                                         transform=tr, dtype='int32')

        dep = {m: np.zeros((h, w), 'int32') for m in MODES}
        for (_, m, _, _, _), g in burn(g_dir):
            dep[m] += g
        lines = {m: np.zeros((h, w), 'int16') for m in MODES}
        line_all = np.zeros((h, w), 'int32')
        for (_, m, _, _, _), g in burn(g_line):
            line_all += g
            lines[m] += (g > 0)
        night = np.zeros((h, w), 'int32')
        for _, g in burn(g_night):
            night += g
        nlines = np.zeros((h, w), 'int16')
        for _, g in burn(g_nightl):
            nlines += (g > 0)
        sel = cand[nt_iso]
        newt = (rasterize(zip(geoms[nt_iso[sel]], nt_val[sel]), out_shape=(h, w), transform=tr,
                          dtype='int32') if sel.any() else np.zeros((h, w), 'int32'))

        total = sum(dep[m] for m in MODES)
        todo = bi
        while len(todo):
            # Overlapping buildings (duplicated outlines, parts) hide each other in one raster:
            # the ones left without a cell get another pass on their own
            bid = rasterize(zip(bgeoms[todo], bids[todo]), out_shape=(h, w), transform=tr,
                            dtype='int32', all_touched=True)
            mask = bid > 0
            b = bid[mask] - 1
            if len(b) == 0:
                break
            tot = total[mask]
            # best cell per building: most departures (first one on ties)
            o = np.lexsort((-tot, b))
            bs, first = np.unique(b[o], return_index=True)
            cell = o[first]
            better = tot[cell] > best[bs]
            upd, cu = bs[better], cell[better]
            best[upd] = tot[cu]
            for m in MODES:
                res[f'dep_{m}'][upd] = dep[m][mask][cu]
                res[f'lines_{m}'][upd] = lines[m][mask][cu]
            res['dep_all'][upd] = tot[cu]
            res['lines_all'][upd] = sum(res[f'lines_{m}'][upd] for m in MODES)
            for k, grid in (('line_all', line_all), ('night_dep', night), ('night_lines', nlines),
                            ('newtram', newt)):
                np.maximum.at(res[k], b, grid[mask].astype(res[k].dtype))
            todo = np.setdiff1d(todo, bs)
        if ti % 20 == 0:
            log(f"    tile {ti}/{len(tiles)} ({time.time() - t0:.0f}s)")
    return res


def walk_times(city, bld, stops_mode, cap, log):
    """Minutes from each building to the nearest stop of each mode (same network as 03 --gates
    --major-barriers), capped at `cap`."""
    from scipy.sparse import csr_matrix, hstack, vstack
    from scipy.sparse.csgraph import dijkstra
    from scipy.spatial import cKDTree
    iso = _load('03_generate_isochrones_local')
    crs, osm = city['crs_metric'], city['osm_dir']
    with open(city['network_dir'] / 'walking_network.pkl', 'rb') as f:
        G = pickle.load(f)
    access = {'closed_gates': closed_gate_ids(osm, crs), 'private_ways': private_way_ids(osm)}
    _, _, coords, _, _, gn = iso.convert_to_sparse(G, crs, access, None, major=True)
    del G
    two = gn['two_layer'].tocsr()
    nn = len(coords)
    # building -> nearest node (public or residents-only) + straight distance to it
    rp = bld.geometry.representative_point()
    pts = np.column_stack([rp.x, rp.y])
    d_snap, k = cKDTree(coords).query(pts)
    node_pt = gpd.GeoSeries(gpd.points_from_xy(coords[k, 0], coords[k, 1]), crs=bld.crs)
    d_snap = bld.geometry.distance(node_pt, align=False).to_numpy()  # 0 if the node is inside
    limit = cap * WALK_M_PER_MIN
    out = {}
    for m, st in stops_mode.items():
        sx, sy = st.geometry.x.to_numpy(), st.geometry.y.to_numpy()
        lonlat = np.column_stack([st['stop_lon'], st['stop_lat']])
        _, kk = gn['public_tree'].query(lonlat)
        src = gn['public_idx'][kk]
        snap = np.hypot(coords[src, 0] - sx, coords[src, 1] - sy)
        # virtual source node S (index 2N) linked to every stop's node by its snap distance
        S = csr_matrix((snap + 1e-3, (np.zeros(len(src), int), src)), shape=(1, 2 * nn))
        A = vstack([hstack([two, csr_matrix((2 * nn, 1))]), hstack([S, csr_matrix((1, 1))])],
                   format='csr')
        d = dijkstra(A, directed=True, indices=2 * nn, limit=limit)
        dn = np.minimum(d[k], d[nn + k]) + d_snap
        out[f'walk_{m}'] = np.round(np.minimum(dn / WALK_M_PER_MIN, cap), 1)
        log(f"  walk_{m}: {len(src):,} stops, median {np.median(out[f'walk_{m}']):.1f} min")
    out['walk_any'] = np.min(np.column_stack([out[f'walk_{m}'] for m in MODES]), axis=1)
    return out


def main():
    ap = argparse.ArgumentParser(description='Per-building transit access by mode')
    add_city_argument(ap)
    ap.add_argument('--date', required=True, help='GTFS date YYYYMMDD (as used for 01)')
    ap.add_argument('--variant', default='gates_mb', help='isochrones_<variant>[_<N>min].gpkg')
    ap.add_argument('--minutes', type=int, nargs='+', default=[5, 10])
    ap.add_argument('--new-tram', help='CSV stop_id,opened(YYYYMMDD),section')
    ap.add_argument('--compare', default='buildings_gates.gpkg', help='05 output to compare max_trips with')
    ap.add_argument('--check-variant', default='gates',
                    help='Also run the line rule on these (older) isochrones: checks the method vs max_trips')
    ap.add_argument('--cap', type=float, default=30)
    ap.add_argument('--out', default='buildings_modes.gpkg')
    ap.add_argument('--test-area', help='lat,lon,radius_m: only buildings there (quick test run)')
    ap.add_argument('data_folder')
    args = ap.parse_args()
    city = get_city(args.city)
    crs = city['crs_metric']
    data = Path(args.data_folder)
    out_file = data / args.out
    report = []

    def log(msg):
        print(msg, flush=True)
        report.append(msg)

    t0 = time.time()
    line, dirr, nightr = departures(city, data, args.date)
    log(f"Departures 06-22 (line rule) {line['n'].sum():,}; boardable (direction rule) "
        f"{dirr['n'].sum():,}; night N-lines {nightr['n'].sum():,}")
    log("  by mode (direction rule): " + ", ".join(
        f"{m} {dirr.loc[dirr['mode'] == m, 'n'].sum():,}" for m in MODES))
    newtram = (pd.read_csv(args.new_tram, dtype={'stop_id': str}) if args.new_tram else
               pd.DataFrame({'stop_id': pd.Series([], dtype=str), 'opened': pd.Series([], dtype=int)}))

    tmax = max(args.minutes)
    iso_name = lambda v, t, res='': f"isochrones_{v}{res}" + (f"_{t}min" if t != 5 else "") + ".gpkg"
    # residents files: 03 names them isochrones_<variant-head>_residents<tag>
    def iso_files(v, t):
        head, _, tag = v.partition('_')
        pub = data / iso_name(v, t)
        res = data / (f"isochrones_{head}_residents" + (f"_{tag}" if tag else "")
                      + (f"_{t}min" if t != 5 else "") + ".gpkg")
        return pub, res

    print("Loading buildings...", flush=True)
    bounds_file = iso_files(args.variant, tmax)[0]
    ib = gpd.read_file(bounds_file, columns=['stop_id']).total_bounds
    b = gpd.read_file(city['osm_dir'] / 'buildings.gpkg', bbox=tuple(ib))
    if 'building' in b.columns:
        b = b[~b['building'].isin(EXCLUDED_BUILDING_TYPES)]
    b = b[b.geometry.notna() & b.geom_type.isin(['Polygon', 'MultiPolygon'])].to_crs(crs)
    if args.test_area:
        lat, lon, rad = (float(v) for v in args.test_area.split(','))
        c = gpd.GeoSeries.from_xy([lon], [lat], crs=4326).to_crs(crs).iloc[0]
        b = b[b.geometry.distance(c) < rad]
    b = b.reset_index(drop=True)
    log(f"Buildings: {len(b):,}")
    out = gpd.GeoDataFrame({'osm_id': b['osm_id']}, geometry=b.geometry, crs=crs)
    out['area_m2'] = b.geometry.area.round(0)
    out['big'] = out['area_m2'] > BIG_BUILDING_M2

    def run(pub_file, res_file):
        log(f"  {pub_file.name}")
        r = per_building(b, gpd.read_file(pub_file).to_crs(crs), line, dirr, nightr, newtram, log)
        r['residents_only'] = np.zeros(len(b), bool)
        if res_file.exists():
            log(f"  {res_file.name} (buildings not reached publicly)")
            rr = per_building(b, gpd.read_file(res_file).to_crs(crs), line, dirr, nightr, newtram, log)
            only = (r['dep_all'] == 0) & (r['line_all'] == 0) & (rr['line_all'] > 0)
            for k in rr:
                r[k] = np.where(only, rr[k], r[k])
            r['residents_only'] = only
        return r

    for t in args.minutes:
        log(f"{t}-min walk:")
        r = run(*iso_files(args.variant, t))
        for m in MODES + ['all']:
            out[f'dep{t}_{m}'] = r[f'dep_{m}']
        for m in MODES + ['all']:
            out[f'lines{t}_{m}'] = r[f'lines_{m}']
        out[f'line{t}_all'] = r['line_all']
        out[f'night{t}_dep'] = r['night_dep']
        out[f'night{t}_lines'] = r['night_lines']
        out[f'newtram{t}'] = r['newtram']
        out[f'residents_only{t}'] = r['residents_only']
        assert (out[[f'dep{t}_{m}' for m in MODES]].sum(axis=1) == out[f'dep{t}_all']).all()

    stops = pd.read_csv(data / 'stops_trip_count.csv', dtype={'stop_id': str})
    sg = gpd.GeoDataFrame(stops, geometry=gpd.points_from_xy(stops.stop_lon, stops.stop_lat),
                          crs=4326).to_crs(crs)
    served = dirr.groupby(['stop_id', 'mode'])['n'].sum().unstack(fill_value=0)
    stops_mode = {m: sg[sg['stop_id'].isin(served.index[served.get(m, 0) > 0])] for m in MODES}
    log("Walking times to the nearest stop by mode:")
    for k, v in walk_times(city, b, stops_mode, args.cap, log).items():
        out[k] = v

    # --- comparison with 05's max_trips ---
    cmp_file = data / args.compare
    if cmp_file.exists():
        old = gpd.read_file(cmp_file, columns=['osm_id', 'max_trips', 'residents_only'], ignore_geometry=True)
        log(f"\nComparison with {cmp_file.name} (max_trips; {len(old):,} buildings):")
        if args.check_variant:
            pub, res = iso_files(args.check_variant, 5)
            if pub.exists():
                log(f"Method check: line rule on {pub.name} (the isochrones max_trips came from):")
                rc = run(pub, res)
                out['check5_line_all'] = rc['line_all']
        m = out.drop(columns='geometry').merge(old, on='osm_id', how='inner', suffixes=('', '_05'))
        log(f"  matched by osm_id: {len(m):,}")

        def stats(col, label):
            d = m[col] - m['max_trips']
            rel = d.abs() / m['max_trips'].clip(lower=1)
            log(f"  {label}: exact {(d == 0).mean():.1%}, within 5 % {(rel <= 0.05).mean():.1%}, "
                f"median {m[col].median():.0f} vs {m['max_trips'].median():.0f}, "
                f"mean ratio {(m[col].sum() / m['max_trips'].sum()):.3f}, "
                f"corr {np.corrcoef(m[col], m['max_trips'])[0, 1]:.4f}")
        if 'check5_line_all' in m:
            stats('check5_line_all', 'same isochrones, same rule (method check)')
        stats('line5_all', 'new isochrones, same rule (routing fixes + road/rail barriers)')
        stats('dep5_all', 'new isochrones, per direction, departures only (dep5_all)')

    out.to_crs(4326).to_file(out_file, driver='GPKG', layer='buildings_modes')
    log(f"\nSaved {out_file} ({len(out):,} buildings, {time.time() - t0:.0f}s)")
    out_file.with_name(out_file.stem + '_report.txt').write_text('\n'.join(report), encoding='utf-8')


if __name__ == '__main__':
    main()
