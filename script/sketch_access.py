"""
Diagnostic "access sketch" around a stop: what the walking model sees there.

Draws the stop's isochrones (public = dark, residents-only = pale) with buildings,
fences/walls (green), private ways (orange), closed gates (red x) and the stops (*).
Handy for checking why an area is or isn't covered.

Usage:
    python sketch_access.py --city krakow --stop "Cichy Kącik"
    python sketch_access.py --city warsaw --stop "Centrum" --radius 600
    -> png/previews/sketch_<city>_<stop>.png

Needs isochrones_gates*.gpkg (03 --gates) in the latest data folder for the city.
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


def main():
    parser = argparse.ArgumentParser(description='Access sketch around a stop')
    add_city_argument(parser)
    parser.add_argument('--stop', required=True, help='Stop name (substring, case-insensitive)')
    parser.add_argument('--radius', type=float, default=450, help='Half-width of the view in m')
    parser.add_argument('--highlight', help='gpkg of polygons to outline in blue (e.g. uncovered plots)')
    parser.add_argument('--suffix', default='', help='Appended to the output file name')
    parser.add_argument('data_folder', nargs='?')
    args = parser.parse_args()

    city = get_city(args.city)
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
    r = args.radius
    bb = (cx - r, cy - r, cx + r, cy + r)
    b4 = tuple(gpd.GeoSeries.from_xy([bb[0], bb[2]], [bb[1], bb[3]], crs=crs).to_crs(4326).total_bounds)

    pub = gpd.read_file(data_dir / 'isochrones_gates.gpkg').to_crs(crs)
    pub = pub[pub.stop_id.isin(ids)]
    res_file = data_dir / 'isochrones_gates_residents.gpkg'
    res = gpd.read_file(res_file).to_crs(crs) if res_file.exists() else pub.iloc[0:0]
    res = res[res.stop_id.isin(ids)]

    fig, ax = plt.subplots(figsize=(11, 11))
    bld = gpd.read_file(osm / 'buildings.gpkg', bbox=b4).to_crs(crs)
    bld.plot(ax=ax, color='#d4d4d4')
    # One shape per class (stacked translucent stops would blur pale into dark)
    pub_u = pub.geometry.unary_union if len(pub) else None
    if len(res):
        res_only = res.geometry.unary_union
        if pub_u is not None:
            res_only = res_only.difference(pub_u)
        gpd.GeoSeries([res_only], crs=crs).plot(ax=ax, color='#f8cfe0')
    if pub_u is not None:
        gpd.GeoSeries([pub_u], crs=crs).plot(ax=ax, color='#d96b97')
    bld.boundary.plot(ax=ax, color='#4a4a4a', lw=0.6)  # outlines on top of the isochrones
    fen = gpd.read_file(osm / 'barriers.gpkg', bbox=b4).to_crs(crs)
    if len(fen):
        fen.boundary.where(fen.geom_type == 'Polygon', fen.geometry).plot(ax=ax, color='#1b5e20', lw=1)
    pw = gpd.read_file(osm / 'private_ways.gpkg', bbox=b4).to_crs(crs)
    priv = pw[pw.osm_id.astype('int64').isin(private_way_ids(osm))]
    if len(priv):
        priv.plot(ax=ax, color='orange', lw=2)
    gt = gpd.read_file(osm / 'gates.gpkg', bbox=b4).to_crs(crs)
    closed = gt[gt.osm_id.astype('int64').isin(closed_gate_ids(osm, crs))]
    if len(closed):
        closed.plot(ax=ax, color='red', marker='x', markersize=60)
    if args.highlight:
        hl = gpd.read_file(args.highlight, bbox=b4).to_crs(crs)
        if len(hl):
            hl.plot(ax=ax, facecolor='none', edgecolor='#1565c0', hatch='///', lw=1.2, zorder=4)
    stops.plot(ax=ax, color='blue', markersize=80, marker='*', zorder=5)
    ax.set_xlim(bb[0], bb[2])
    ax.set_ylim(bb[1], bb[3])
    ax.set_axis_off()
    name = st['stop_name'].iloc[0]
    ax.set_title(f'{name} ({city["name"]}): dark = public isochrones, pale = residents-only, '
                 f'green = fences, orange = private ways, red x = closed gates, * = stops'
                 + (', blue hatch = highlighted' if args.highlight else ''), fontsize=9)
    import unicodedata
    ascii_name = unicodedata.normalize('NFKD', name.lower().replace('ł', 'l')).encode('ascii', 'ignore').decode()
    slug = re.sub(r'[^a-z0-9]+', '_', ascii_name).strip('_')
    out = PROJECT_DIR / 'png' / 'previews' / f'sketch_{city["key"]}_{slug}{args.suffix}.png'
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=90, bbox_inches='tight')
    print(f'Saved {out}')


if __name__ == '__main__':
    main()
