"""
Diagnostic "access sketch" around a stop: what the walking model sees there.

Draws the stop's isochrones (public = dark, residents-only = pale) with buildings,
fences/walls (green), private ways (orange), closed gates (red x) and the stops (*).
Handy for checking why an area is or isn't covered.

Usage:
    python sketch_access.py --city krakow --stop "Cichy Kącik"
    python sketch_access.py --city warsaw --stop "Centrum" --radius 600
    -> png/previews/sketch_<city>_<stop>.png

Needs isochrones_gates*.gpkg (03 --gates) in the latest data folder for the city,
or --recompute: rebuild just these stops' isochrones with the current access rules
and barrier model (loads the walking network, ~1-3 min) - for checking a rule change
before rerunning the whole city.
"""
import argparse
import re
from pathlib import Path

import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import pandas as pd

from access_rules import closed_gate_ids, private_way_ids
from cities import get_city, add_city_argument, PROJECT_DIR


def recompute(city, st, stops):
    """Isochrones of just these stops with the current rules (as 03 --gates does)."""
    import importlib.util
    import pickle
    import numpy as np
    spec = importlib.util.spec_from_file_location(
        'iso', Path(__file__).resolve().parent / '03_generate_isochrones_local.py')
    iso = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(iso)
    crs, osm = city['crs_metric'], city['osm_dir']
    with open(city['network_dir'] / 'walking_network.pkl', 'rb') as f:
        G = pickle.load(f)
    access = {'closed_gates': closed_gate_ids(osm, crs), 'private_ways': private_way_ids(osm)}
    dem = None if city.get('_flat') else iso.load_dem(city)   # --flat: compare without hills
    iso.set_minutes(city.get('_minutes', iso.TIME_LIMIT))
    major = bool(city.get('_major'))
    _, _, coords, _, _, gate_net = iso.convert_to_sparse(G, crs, access, dem, major=major)
    del G
    grid = iso.BarrierGrid(city, crs, coords, access['closed_gates'], major=gate_net['major'])
    ctx = {'coords': coords, 'grid': grid, 'gate_net': gate_net, 'sparse': None}
    _, k = gate_net['public_tree'].query(np.column_stack([st.stop_lon, st.stop_lat]))
    isls = iso.island_starts(gate_net, coords, stops.geometry.x.to_numpy(), stops.geometry.y.to_numpy())
    pubs, ress = [], []
    for sid, p, nn, isl in zip(st.stop_id, stops.geometry, gate_net['public_idx'][k], isls):
        poly, poly_res = iso.compute_stop(nn, p.x, p.y, ctx, int(isl))
        if poly is not None:
            pubs.append((sid, poly))
            ress.append((sid, poly_res if poly_res is not None else poly))
    as_gdf = lambda rows: gpd.GeoDataFrame({'stop_id': [r[0] for r in rows]},
                                           geometry=[r[1] for r in rows], crs=crs)
    return as_gdf(pubs), as_gdf(ress)


def main():
    parser = argparse.ArgumentParser(description='Access sketch around a stop')
    add_city_argument(parser)
    parser.add_argument('--stop', required=True, help='Stop name (substring, case-insensitive)')
    parser.add_argument('--radius', type=float, default=450, help='Half-width of the view in m')
    parser.add_argument('--highlight', help='gpkg of polygons to outline in blue (e.g. uncovered plots)')
    parser.add_argument('--name', help='Title / file name (default: first matching stop)')
    parser.add_argument('--flat', action='store_true',
                        help='With --recompute: ignore hills (compare slope-aware walking with flat)')
    parser.add_argument('--recompute', action='store_true',
                        help='Recompute these stops with the current rules instead of reading the city files')
    parser.add_argument('--suffix', default='', help='Appended to the output file name')
    parser.add_argument('--center', help='View centre "lat,lon" (default: mean of the stops)')
    parser.add_argument('--paths', choices=['all', 'streets', 'none'], default='all',
                        help='all = public paths green + private orange; streets = pale streets only; none')
    parser.add_argument('--building-status', action='store_true',
                        help='Fill buildings by result: reached (dark), residents-only (mid), not reached (hatch)')
    parser.add_argument('--flat-outline', action='store_true',
                        help='Hill cities: also outline the walk area as if the city were flat (dashed)')
    parser.add_argument('--steps', action='store_true', help='Draw stairs (highway=steps)')
    parser.add_argument('--major-barriers', action='store_true',
                        help='With --recompute: major roads/railways as barriers (03 --major-barriers)')
    parser.add_argument('--mb-outline', action='store_true',
                        help='With --major-barriers: also outline the walk area without them (dashed)')
    parser.add_argument('--minutes', type=float, default=5, help='With --recompute: walking time')
    parser.add_argument('--dpi', type=int, default=90)
    parser.add_argument('--no-legend', action='store_true', help='No legend box (for panels)')
    parser.add_argument('--cached', action='store_true',
                        help='Reuse the isochrones the last --recompute of these stops saved')
    parser.add_argument('data_folder', nargs='?')
    args = parser.parse_args()

    city = get_city(args.city)
    city['_flat'] = args.flat
    city['_major'] = args.major_barriers
    city['_minutes'] = args.minutes
    crs = city['crs_metric']
    osm = city['osm_dir']
    data_dir = Path(args.data_folder) if args.data_folder else sorted(
        d for d in city['data_dir'].iterdir() if (d / 'isochrones_gates.gpkg').exists())[-1]

    st = pd.read_csv(data_dir / 'stops_trip_count.csv', dtype={'stop_id': str})
    st = st[st['stop_name'].str.contains(args.stop, case=False, na=False)]
    if st.empty:
        raise SystemExit(f"No stop matching '{args.stop}'")
    ids = set(st['stop_id'])
    stops = gpd.GeoDataFrame(st, geometry=gpd.points_from_xy(st.stop_lon, st.stop_lat), crs=4326).to_crs(crs)
    cx, cy = stops.geometry.x.mean(), stops.geometry.y.mean()
    if args.center:
        lat, lon = (float(v) for v in args.center.split(','))
        cx, cy = gpd.GeoSeries.from_xy([lon], [lat], crs=4326).to_crs(crs).iloc[0].coords[0]
    r = args.radius
    bb = (cx - r, cy - r, cx + r, cy + r)
    b4 = tuple(gpd.GeoSeries.from_xy([bb[0], bb[2]], [bb[1], bb[3]], crs=crs).to_crs(4326).total_bounds)

    cache = data_dir / 'diagnostics' / ('sketch_iso_' + re.sub(r'[^a-z0-9]+', '_', args.stop.lower()).strip('_') + '.gpkg')
    if args.cached:
        pub = gpd.read_file(cache, layer='public').to_crs(crs)
        res = gpd.read_file(cache, layer='residents').to_crs(crs)
        import fiona
        flat = gpd.read_file(cache, layer='flat').to_crs(crs) if 'flat' in fiona.listlayers(cache) else None
    elif args.recompute:
        pub, res = recompute(city, st, stops)
        cache.parent.mkdir(exist_ok=True)
        pub.to_file(cache, layer='public', driver='GPKG')
        (res if len(res) else pub.iloc[0:0]).to_file(cache, layer='residents', driver='GPKG')
        flat = None
        if args.flat_outline:
            flat, _ = recompute({**city, '_flat': True}, st, stops)
            flat.to_file(cache, layer='flat', driver='GPKG')
        elif args.mb_outline:   # the dashed outline = same walk without road/rail barriers
            flat, _ = recompute({**city, '_major': False}, st, stops)
            flat.to_file(cache, layer='flat', driver='GPKG')
    else:
        pub = gpd.read_file(data_dir / 'isochrones_gates.gpkg').to_crs(crs)
        pub = pub[pub.stop_id.isin(ids)]
        res_file = data_dir / 'isochrones_gates_residents.gpkg'
        res = gpd.read_file(res_file).to_crs(crs) if res_file.exists() else pub.iloc[0:0]
        res = res[res.stop_id.isin(ids)]

    fig, ax = plt.subplots(figsize=(11, 11))
    bld = gpd.read_file(osm / 'buildings.gpkg', bbox=b4).to_crs(crs)
    # One shape per class (stacked translucent stops would blur pale into dark)
    pub_u = pub.geometry.unary_union if len(pub) else None
    if len(res):
        res_only = res.geometry.unary_union
        if pub_u is not None:
            res_only = res_only.difference(pub_u)
        gpd.GeoSeries([res_only], crs=crs).plot(ax=ax, color='#f0c8e6' if args.building_status else '#f8cfe0')
    if pub_u is not None:
        gpd.GeoSeries([pub_u], crs=crs).plot(ax=ax, color='#c6b6ea' if args.building_status else '#d96b97')
    plt.rcParams['hatch.linewidth'] = 0.6
    if args.building_status:
        # A building counts as reached when the walk area touches it (as in 05)
        hit_pub = bld.intersects(pub_u) if pub_u is not None else pd.Series(False, index=bld.index)
        res_u = res.geometry.unary_union if len(res) else None
        hit = hit_pub | (bld.intersects(res_u) if res_u is not None else False)
        # One colour for every reached building: the surface shows who may walk there
        bld[hit].plot(ax=ax, color='#5b2c8f')
        bld[~hit].plot(ax=ax, facecolor='white', edgecolor='#a8a8a8', hatch='//////', lw=0)
    else:
        # Buildings as a pale grey hatch on top of the isochrones (the walk area shows through)
        bld.plot(ax=ax, facecolor='none', edgecolor='#a8a8a8', hatch='//////', lw=0)
    bld.boundary.plot(ax=ax, color='#7a7a7a', lw=0.5)  # outlines on top of the isochrones
    fen = gpd.read_file(osm / 'barriers.gpkg', bbox=b4).to_crs(crs)
    if len(fen):
        fen.boundary.where(fen.geom_type.isin(['Polygon', 'MultiPolygon']), fen.geometry).plot(ax=ax, color='#263238', lw=1.0)
    pw = gpd.read_file(osm / 'private_ways.gpkg', bbox=b4).to_crs(crs)  # all minor ways (footways, service, ...)
    is_priv = pw.osm_id.astype('int64').isin(private_way_ids(osm))
    priv = pw[is_priv]
    # Public walkable ways in green: streets + the public minor ways
    rd = gpd.read_file(osm / 'roads.gpkg', bbox=b4).to_crs(crs)
    rd_all = rd
    rd = rd[~rd.highway.isin(['motorway', 'motorway_link', 'trunk', 'trunk_link'])]
    lines = ['LineString', 'MultiLineString']
    if args.paths == 'all':
        paths = pd.concat([rd.geometry, pw[~is_priv].geometry])
        paths[paths.geom_type.isin(lines)].plot(ax=ax, color='#2e9d4a', lw=1.1)
    elif args.paths == 'streets':
        rd[rd.geom_type.isin(lines)].plot(ax=ax, color='#9e9e9e', lw=1.0, alpha=0.6, zorder=1.5)
    if len(priv) and args.paths == 'all':
        priv.plot(ax=ax, color='orange', lw=2)
    gt = gpd.read_file(osm / 'gates.gpkg', bbox=b4).to_crs(crs)
    closed = gt[gt.osm_id.astype('int64').isin(closed_gate_ids(osm, crs))]
    opened = gt[~gt.index.isin(closed.index)]
    if len(opened):
        opened.plot(ax=ax, color='#263238', marker='o', markersize=14, zorder=4)
    if len(closed):
        closed.plot(ax=ax, color='#e53935', marker='s', markersize=16, zorder=4)
    if args.highlight:
        hl = gpd.read_file(args.highlight, bbox=b4).to_crs(crs)
        if len(hl):
            hl.plot(ax=ax, facecolor='none', edgecolor='#1565c0', hatch='///', lw=1.2, zorder=4)
    if args.major_barriers:   # the barrier lines: major roads + railways
        mj = rd_all[rd_all.highway.isin(['primary', 'primary_link', 'secondary', 'secondary_link',
                                 'trunk', 'trunk_link'])]
        mj = mj[mj.geom_type.isin(lines)]
        if len(mj):
            mj.plot(ax=ax, color='#c62828', lw=2.2, alpha=0.75, zorder=3)
        rw = gpd.read_file(osm / 'railways.gpkg', bbox=b4).to_crs(crs)
        rw = rw[rw.railway.isin(['rail', 'narrow_gauge', 'light_rail']) & rw.tunnel.isna()]
        if len(rw):
            rw.plot(ax=ax, color='#1a1a2e', lw=2.0, linestyle=(0, (3, 1.5)), zorder=3)
    if args.steps:
        stp = pw[pw.highway == 'steps']
        if len(stp):
            stp.plot(ax=ax, color='#1a1a2e', lw=2.4, linestyle=(0, (0.6, 0.6)), zorder=3)
    if 'flat' in locals() and flat is not None and len(flat):
        gpd.GeoSeries([flat.geometry.unary_union.boundary], crs=crs).plot(
            ax=ax, color='#1a1a2e', lw=1.3, linestyle=(0, (5, 3)), zorder=3.5)
    stops.plot(ax=ax, color='#f5b700', markersize=110, marker='o', zorder=5)
    ax.set_xlim(bb[0], bb[2])
    ax.set_ylim(bb[1], bb[3])
    ax.set_axis_off()
    name = args.name or st['stop_name'].iloc[0]
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    bs = args.building_status
    mins = f'{args.minutes:g}-min'
    handles = [Patch(color='#c6b6ea' if bs else '#d96b97', label=f'{mins} walk, public'),
               Patch(color='#f0c8e6' if bs else '#f8cfe0', label=f'{mins} walk, residents only')]
    if bs:
        handles += [Patch(color='#5b2c8f', label='building reached'),
                    Patch(facecolor='white', edgecolor='#a8a8a8', hatch='//////', label='building not reached')]
    if args.paths == 'all':
        handles += [Line2D([], [], color='#2e9d4a', lw=1.5, label='public path'),
                    Line2D([], [], color='orange', lw=2, label='private way')]
    handles += [Line2D([], [], color='#263238', lw=1.2, label='fence / wall'),
                Line2D([], [], ls='', marker='s', ms=5, mfc='#e53935', mec='#e53935', label='closed gate'),
                Line2D([], [], ls='', marker='o', ms=5, mfc='#263238', mec='#263238', label='open gate'),
                Line2D([], [], ls='', marker='o', ms=10, mfc='#f5b700', mec='#f5b700', label='stop')]
    if args.steps:
        handles.append(Line2D([], [], color='#1a1a2e', lw=2.4, ls=(0, (0.6, 0.6)), label='stairs'))
    if args.flat_outline:
        handles.append(Line2D([], [], color='#1a1a2e', lw=1.3, ls=(0, (5, 3)), label='5-min walk if it were flat'))
    elif args.mb_outline:
        handles.append(Line2D([], [], color='#1a1a2e', lw=1.3, ls=(0, (5, 3)),
                              label=f'{mins} walk without road/rail barriers'))
    if args.major_barriers:
        handles += [Line2D([], [], color='#c62828', lw=2.2, alpha=0.75, label='major road (barrier)'),
                    Line2D([], [], color='#1a1a2e', lw=2.0, ls=(0, (3, 1.5)), label='railway (barrier)')]
    if args.highlight:
        handles.append(Patch(facecolor='none', edgecolor='#1565c0', hatch='///', label='highlighted'))
    if not args.no_legend:
        ax.legend(handles=handles, loc='lower right', fontsize=9, framealpha=0.93, edgecolor='#cccccc')
    ax.set_title(f'{name} ({city["name"]})', fontsize=13, loc='left')
    import unicodedata
    ascii_name = unicodedata.normalize('NFKD', name.lower().replace('ł', 'l')).encode('ascii', 'ignore').decode()
    slug = re.sub(r'[^a-z0-9]+', '_', ascii_name).strip('_')
    out = PROJECT_DIR / 'png' / 'previews' / f'sketch_{city["key"]}_{slug}{args.suffix}.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    # Temp file outside OneDrive, copied in with retries; timestamped name if still locked
    import os, shutil, tempfile, time
    from datetime import datetime
    fd, tmp = tempfile.mkstemp(suffix='.png')
    os.close(fd)
    fig.savefig(tmp, dpi=args.dpi, bbox_inches='tight')
    for _ in range(6):
        try:
            shutil.copyfile(tmp, out)
            break
        except OSError:
            time.sleep(5)
    else:
        out = out.with_name(f'{out.stem}_{datetime.now():%H%M}.png')
        shutil.copyfile(tmp, out)
    os.remove(tmp)
    print(f'Saved {out}')


if __name__ == '__main__':
    main()
