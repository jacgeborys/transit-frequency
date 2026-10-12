"""
Export a city crop for Blender: building footprints with heights and poster colours, plus
ground layers (water, green, roads, railways), as one JSON file Blender can read without
extra libraries.

    python 3d/export_scene.py --city warsaw --center 52.2318,21.0060 --radius 1500 --name warsaw_center

Writes 3d/scenes/<name>/scene.json (coordinates in metres, local to the crop centre, in the
city's metric CRS; z up). Colours come from render_poster.poster_colours, so they match the
dark poster: palette[0] = not covered, palette[k] = class k (lower < max_trips <= upper).

Heights (Warsaw): D:/QGIS/ceny_mieszkan/output/warsaw_prices_viz.gpkg, layer buildings_3d
(simple buildings + building:part, all with height_m). Other cities: OSM height, else
building_levels x 3.2 m, else 9 m. Parts take the frequency of the footprint they sit in.
"""
import argparse
import json
import sys
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely.geometry import Point, box, MultiPolygon, Polygon

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / 'script'))
from cities import get_city  # noqa: E402
from render_poster import poster_colours  # noqa: E402
import render_qgis_style as rq  # noqa: E402

warnings.filterwarnings('ignore')

HEIGHTS_3D = {'warsaw': (Path(r'D:\QGIS\ceny_mieszkan\output\warsaw_prices_viz.gpkg'), 'buildings_3d')}
LEVEL_M, DEFAULT_M = 3.2, 9.0
ROAD_WIDTH = {  # metres, full width of the drawn strip
    'motorway': 16, 'trunk': 16, 'primary': 13, 'secondary': 11, 'tertiary': 9,
    'motorway_link': 7, 'trunk_link': 7, 'primary_link': 7, 'secondary_link': 7, 'tertiary_link': 7,
    'residential': 7, 'unclassified': 7, 'living_street': 6, 'service': 4, 'pedestrian': 5,
    'footway': 2.5, 'path': 2, 'cycleway': 2.5, 'steps': 2.5, 'track': 3,
}
GREEN = {'parks', 'meadow', 'grass', 'leisure', 'leisure_relations', 'cemeteries', 'allotments'}


def stack_layers(city):
    """The poster's ground layers {stem: (path, layername, subset)} for this city."""
    tmpl = city if 'qgis_layout' in city else get_city(city.get('template', 'krakow'))
    root = rq.load_project(rq.PROJECT_FILE)
    by_id = {ml.find('id').text: ml for ml in root.iter('maplayer')}
    group = rq.find_group(rq.find_group(root.find('layer-tree-group'), 'Tlo'), tmpl['qgis_group'])
    out = {}
    for i in rq.visible_layer_ids(group):
        l = rq.parse_layer(by_id[i], rq.PROJECT_DIR)
        if 'coverage_map' in l['path'].name or l['path'].stem in out:
            continue  # first one wins ('roads copy' = bridges only)
        path = l['path'] if tmpl is city else city['osm_dir'] / l['path'].name
        if path.exists():
            out[l['path'].stem] = (path, l['layername'], l['subset'])
    return out


def read(path, crs, bbox_m, layer=None, subset=None):
    """Features intersecting bbox_m (in crs), reprojected to crs."""
    import fiona
    with fiona.open(path, layer=layer) as src:
        src_crs = src.crs_wkt
    bb = Transformer.from_crs(crs, src_crs, always_xy=True).transform_bounds(*bbox_m)
    gdf = gpd.read_file(path, layer=layer, bbox=bb)
    if subset and len(gdf):
        gdf = gdf.query(rq.subset_to_query(subset))
    return gdf.to_crs(crs)


def rings(geom, cx, cy):
    """Polygon/MultiPolygon -> list of polygons, each [exterior, *holes] as [[x, y], ...]."""
    polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom] if isinstance(geom, Polygon) else []
    out = []
    for p in polys:
        if p.is_empty or p.area < 0.05:  # keep columns and small parts
            continue
        rs = [p.exterior] + list(p.interiors)
        out.append([[[round(x - cx, 2), round(y - cy, 2)] for x, y in r.coords[:-1]] for r in rs])
    return out


def ground_polys(gdf, crop, cx, cy):
    if gdf is None or gdf.empty:
        return []
    g = gdf.geometry
    g = g[g.geom_type.isin(['Polygon', 'MultiPolygon'])].buffer(0)
    u = g.unary_union.intersection(crop) if len(g) else None
    if u is None or u.is_empty:
        return []
    u = u.simplify(0.3)
    parts = u.geoms if hasattr(u, 'geoms') else [u]
    return [r for p in parts if p.geom_type in ('Polygon', 'MultiPolygon') for r in rings(p, cx, cy)]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--city', default='warsaw')
    ap.add_argument('--center', required=True, help='lat,lon')
    ap.add_argument('--radius', type=float, default=1500, help='Half-side (square) or radius (circle), m')
    ap.add_argument('--shape', choices=['square', 'circle'], default='square')
    ap.add_argument('--bounds', help='xmin,ymin,xmax,ymax in m relative to --center (overrides --radius); '
                                     'the centre stays the scene origin, so saved views still fit')
    ap.add_argument('--data', help='Data folder with buildings_gates.gpkg (default: latest)')
    ap.add_argument('--name', required=True)
    ap.add_argument('--big-grey', action='store_true',
                    help="Big buildings (malls, stations, PKIN) grey as on the poster; default: by frequency")
    args = ap.parse_args()

    city = get_city(args.city)
    crs = city['crs_metric']
    lat, lon = (float(v) for v in args.center.split(','))
    cx, cy = Transformer.from_crs('EPSG:4326', crs, always_xy=True).transform(lon, lat)
    r = args.radius
    bbox_m = (cx - r, cy - r, cx + r, cy + r)
    if args.bounds:
        x0, y0, x1, y1 = (float(v) for v in args.bounds.split(','))
        bbox_m = (cx + x0, cy + y0, cx + x1, cy + y1)
        args.shape = 'square'
    crop = box(*bbox_m) if args.shape == 'square' else Point(cx, cy).buffer(r, 128)
    data = Path(args.data) if args.data else sorted(
        d for d in city['data_dir'].iterdir() if (d / 'buildings_gates.gpkg').exists())[-1]
    print(f"{city['name']}: {(bbox_m[2] - bbox_m[0]) / 1000:g} x {(bbox_m[3] - bbox_m[1]) / 1000:g} km "
          f"{args.shape} around {lat}, {lon}; data {data.name}")

    # Frequency per footprint (same file as the poster)
    freq = read(data / 'buildings_gates.gpkg', crs, bbox_m)
    freq['osm_id'] = freq['osm_id'].astype('int64')
    ug = city['osm_dir'] / 'underground_ids.txt'
    underground = {int(v) for v in ug.read_text().split()} if ug.exists() else set()

    # Geometry + heights
    if city['key'] in HEIGHTS_3D:
        path, layer = HEIGHTS_3D[city['key']]
        b = read(path, crs, bbox_m, layer=layer)
        b = b.rename(columns={'height_m': 'z1', 'min_height_m': 'z0'})
    else:
        b = read(city['osm_dir'] / 'buildings.gpkg', crs, bbox_m)
        h = pd.to_numeric(b.get('height'), errors='coerce')
        lv = pd.to_numeric(b.get('building_levels'), errors='coerce')
        b['z1'] = h.fillna(lv * LEVEL_M).fillna(DEFAULT_M)
        b['z0'] = pd.to_numeric(b.get('min_height'), errors='coerce')
    b['osm_id'] = b['osm_id'].astype('int64')
    b = b[~b['osm_id'].isin(underground) & b.geometry.notna()]
    b = b[b.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])]
    b['z0'] = b['z0'].fillna(0.0).clip(lower=0)
    b['z1'] = b['z1'].fillna(DEFAULT_M)
    b = b[b['z1'] > b['z0'] + 0.1]
    rp = b.geometry.representative_point()
    b = b[rp.within(crop)]

    # max_trips: own osm_id, else the footprint the part's point lies in
    f_idx = freq.set_index('osm_id')
    b['trips'] = b['osm_id'].map(f_idx['max_trips'])
    b['big'] = b['osm_id'].map(f_idx['big'])
    miss = b['trips'].isna()
    if miss.any():
        pts = gpd.GeoDataFrame(geometry=b.loc[miss].geometry.representative_point(), crs=crs)
        j = gpd.sjoin(pts, freq[['max_trips', 'big', 'geometry']], predicate='within', how='left')
        j = j.groupby(level=0).agg({'max_trips': 'max', 'big': 'max'})
        b.loc[miss, 'trips'] = j['max_trips']
        b.loc[miss, 'big'] = j['big']
    print(f"  {len(b):,} buildings/parts; frequency by id {(~miss).sum():,}, "
          f"by location {(miss & b['trips'].notna()).sum():,}, none {b['trips'].isna().sum():,}")
    b['trips'] = b['trips'].fillna(0).astype(int)
    b['big'] = b['big'].fillna(0).astype(bool)

    col = poster_colours(city['key'])
    classes = col['classes']
    palette = [col['uncovered']] + [c for _, _, c in classes]
    uppers = np.array([hi for _, hi, _ in classes])
    lowers = np.array([lo for lo, _, _ in classes])
    t = b['trips'].to_numpy()
    cls = np.searchsorted(uppers, t, side='left') + 1          # lower < t <= upper
    cls = np.where((t > lowers[0]) & ~(args.big_grey & b['big'].to_numpy()), np.minimum(cls, len(classes)), 0)

    buildings = []
    for geom, z0, z1, c, tr in zip(b.geometry, b['z0'], b['z1'], cls, t):
        for poly in rings(geom, cx, cy):
            buildings.append({'rings': poly, 'z0': round(float(z0), 2), 'z1': round(float(z1), 2),
                              'c': int(c), 'trips': int(tr)})

    # Ground layers, from the same files as the poster
    layers = stack_layers(city)
    def load(stem):
        if stem not in layers:
            return None
        p, ln, sub = layers[stem]
        return read(p, crs, bbox_m, layer=ln, subset=sub)
    ground = {'water': ground_polys(load('water'), crop, cx, cy)}
    greens = [g for g in (load(s) for s in sorted(GREEN & set(layers))) if g is not None and len(g)]
    ground['green'] = ground_polys(pd.concat(greens) if greens else None, crop, cx, cy)
    ground['forest'] = ground_polys(load('forests'), crop, cx, cy)
    roads = load('roads')
    if roads is not None and len(roads):
        tun = roads['tunnel'].fillna('no').isin(['no', '']) if 'tunnel' in roads else True
        roads = roads[tun & roads['highway'].isin(ROAD_WIDTH.keys())]
        roads = roads.set_geometry(roads.geometry.buffer(roads['highway'].map(ROAD_WIDTH) / 2, cap_style=2))
        ground['roads'] = ground_polys(roads, crop, cx, cy)
    rail = load('railways')
    if rail is not None and len(rail):
        if 'tunnel' in rail:
            rail = rail[~rq._is_tunnel(rail)]
        ground['railways'] = ground_polys(rail.set_geometry(rail.geometry.buffer(1.5, cap_style=2)), crop, cx, cy)
    ground['base'] = rings(crop, cx, cy)

    out_dir = HERE / 'scenes' / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    scene = {'city': city['key'], 'crs': crs, 'center_lonlat': [lon, lat], 'center_xy': [cx, cy],
             'radius': r, 'bbox_local': [round(v, 1) for v in (bbox_m[0] - cx, bbox_m[1] - cy, bbox_m[2] - cx, bbox_m[3] - cy)], 'shape': args.shape, 'data': str(data),
             'palette': palette, 'class_labels': ['0'] + [f'{lo:g}-{hi:g}' for lo, hi, _ in classes],
             'colours': {k: v for k, v in col.items() if k != 'classes'},
             'buildings': buildings, 'ground': ground}
    out = out_dir / 'scene.json'
    out.write_text(json.dumps(scene, separators=(',', ':')))
    print(f"  wrote {out} ({out.stat().st_size / 1e6:.1f} MB): {len(buildings):,} building polygons, "
          + ', '.join(f'{k} {len(v)}' for k, v in ground.items()))
    print('  class counts: ' + ' '.join(f'{k}:{n}' for k, n in zip(*np.unique(cls, return_counts=True))))


if __name__ == '__main__':
    main()
