"""
Export a city crop for Blender: building footprints with heights and poster colours, plus
ground layers (water, green, roads, railways), as one JSON file Blender can read without
extra libraries.

    python 3d/export_scene.py --city warsaw --center 52.2318,21.0060 --radius 1500 --name warsaw_center

Writes 3d/scenes/<name>/scene.json (coordinates in metres, local to the crop centre, in the
city's metric CRS; z up). Colours come from render_poster.poster_colours, so they match the
dark poster: palette[0] = not covered, palette[k] = class k (lower < max_trips <= upper).

Buildings and heights: the city's OSM basemap (buildings.gpkg, incl. building:part).
height, else building_levels x 3.2 m, else 9 m; parts start at min_height / building_min_level.
A building is drawn whole unless its parts cover >= 85 % of it (then only the parts); parts take
the frequency of the building they belong to, so a landmark is one colour. (The housing-price
project's buildings_3d layer, used at first, drops every building that has any part: Warsaw
Spire kept only its crown, Politechnika's main building vanished.)
--buildings <gpkg>: another buildings file (New York: 3d/osm_buildings_3d.py output, since that
basemap kept no height tags). --context-buildings <gpkg>: neutral dark silhouettes without frequency
(New Jersey: outside the analysis, but a void behind Manhattan looks wrong); palette[context_class].
Ground also gets the 2D coverage tint ('coverage_<k>' layers with the poster's composited
class colour + alpha, from coverage_map_gates.gpkg).
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

LEVEL_M, DEFAULT_M = 3.2, 9.0
PARTS_COVER = 0.85          # parent hidden when its parts cover this share of its footprint
FAR_SIMPLIFY_M = 1.0        # outline simplification outside --detail-bounds
CONTEXT_RGB = (36, 37, 42)  # --context-buildings: darker than 'not covered', reads as background
SMALL_GAP_M2 = 60.0         # coverage holes smaller than this are filled (raster noise)
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


MAX_HOLES = 3  # Blender's polygon fill (tessellate_polygon) leaves polygons with many holes empty


def split_holes(p, max_holes=MAX_HOLES):
    """Halve a polygon (across its longer side) until every piece has <= max_holes holes.
    A coverage class around PKIN was one polygon with 96 holes and rendered without its tint."""
    if len(p.interiors) <= max_holes:
        return [p]
    x0, y0, x1, y1 = p.bounds
    halves = (box(x0, y0, (x0 + x1) / 2, y1), box((x0 + x1) / 2, y0, x1, y1)) if x1 - x0 >= y1 - y0 \
        else (box(x0, y0, x1, (y0 + y1) / 2), box(x0, (y0 + y1) / 2, x1, y1))
    out = []
    for h in halves:
        part = p.intersection(h)
        for q in (part.geoms if hasattr(part, 'geoms') else [part]):
            if isinstance(q, Polygon) and not q.is_empty:
                out += split_holes(q, max_holes)
    return out


def rings(geom, cx, cy):
    """Polygon/MultiPolygon -> list of polygons, each [exterior, *holes] as [[x, y], ...]
    (split so that no piece has more than MAX_HOLES holes)."""
    polys = geom.geoms if isinstance(geom, MultiPolygon) else [geom] if isinstance(geom, Polygon) else []
    polys = [q for p in polys if not p.is_empty for q in split_holes(p)]
    out = []
    for p in polys:
        if p.is_empty or p.area < 0.05:  # keep columns and small parts
            continue
        rs = [p.exterior] + list(p.interiors)
        out.append([[[round(x - cx, 1), round(y - cy, 1)] for x, y in r.coords[:-1]] for r in rs])
    return out


def ground_polys(gdf, crop, cx, cy, tile=250.0, simplify=0.3):
    """Dissolved layer, cut into tile x tile m pieces. Dissolving a road network gives one huge
    polygon with thousands of holes (the blocks), which Blender's polygon fill gets wrong (it
    filled some holes and painted a sheet over the Vistula); small tiles keep each piece simple."""
    import shapely
    if gdf is None or gdf.empty:
        return []
    g = gdf.geometry
    g = g[g.geom_type.isin(['Polygon', 'MultiPolygon'])].buffer(0)
    g = g[~g.is_empty]
    if not len(g):
        return []
    geoms, sidx = g.values.data if hasattr(g.values, 'data') else np.asarray(g.values), g.sindex
    x0, y0, x1, y1 = crop.bounds
    out = []
    for x in np.arange(x0, x1, tile):
        for y in np.arange(y0, y1, tile):
            cell = box(x, y, min(x + tile, x1), min(y + tile, y1)).intersection(crop)
            idx = sidx.query(cell, predicate='intersects')
            if not len(idx):
                continue
            u = shapely.union_all(shapely.intersection(geoms[idx], cell))
            if simplify:  # (not for layers that tile each other: borders would shift apart -> gaps)
                u = shapely.make_valid(u.simplify(simplify))
            parts = u.geoms if hasattr(u, 'geoms') else [u]
            out += [r for p in parts if p.geom_type in ('Polygon', 'MultiPolygon') for r in rings(p, cx, cy)]
    return out


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
    ap.add_argument('--buildings', help="Buildings file (default: the city's osm_basemap buildings.gpkg)")
    ap.add_argument('--context-buildings', help='Buildings drawn as neutral silhouettes (no frequency)')
    ap.add_argument('--detail-bounds', help='xmin,ymin,xmax,ymax (m from the centre): full detail inside; '
                                            'outside, outlines are simplified (far background)')
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
    import re
    data = Path(args.data) if args.data else sorted(  # plain date folders only (not *_previous, *_xl_bbox)
        d for d in city['data_dir'].iterdir()
        if re.fullmatch(r'\d{4}_\d{2}_\d{2}', d.name) and (d / 'buildings_gates.gpkg').exists())[-1]
    print(f"{city['name']}: {(bbox_m[2] - bbox_m[0]) / 1000:g} x {(bbox_m[3] - bbox_m[1]) / 1000:g} km "
          f"{args.shape} around {lat}, {lon}; data {data.name}")

    # Frequency per footprint (same file as the poster)
    freq = read(data / 'buildings_gates.gpkg', crs, bbox_m)
    freq['osm_id'] = freq['osm_id'].astype('int64')
    ug = city['osm_dir'] / 'underground_ids.txt'
    underground = {int(v) for v in ug.read_text().split()} if ug.exists() else set()

    # Geometry + heights (OSM basemap, buildings and building:part)
    b = read(Path(args.buildings) if args.buildings else city['osm_dir'] / 'buildings.gpkg', crs, bbox_m)
    b['osm_id'] = b['osm_id'].astype('int64')
    b = b[~b['osm_id'].isin(underground) & b.geometry.notna()]
    b = b[b.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])].copy()
    num = lambda c: pd.to_numeric(b[c], errors='coerce') if c in b else pd.Series(np.nan, b.index)  # noqa: E731
    h, lv = num('height'), num('building_levels')
    b['z1'] = h.fillna(lv * LEVEL_M).fillna(DEFAULT_M)
    b['z0'] = num('min_height').fillna(num('building_min_level') * LEVEL_M).fillna(0.0).clip(lower=0)
    if 'building' in b:  # roofs/carports without heights: a thin slab at 3.5-4 m, not a 9 m block
        roof = b['building'].isin(['roof', 'carport']) & h.isna()
        b.loc[roof, 'z1'] = 4.0
        b.loc[roof & (b['z0'] == 0), 'z0'] = 3.5
    b = b[b['z1'] > b['z0'] + 0.1]
    b = b[b.geometry.representative_point().within(crop)]
    is_part = b['building_part'].notna() if 'building_part' in b else pd.Series(False, b.index)
    parents, parts = b[~is_part], b[is_part]

    # Which parent does each part belong to (its point in the parent's footprint; largest parent wins)
    # (plain arrays: GeoDataFrame aligns a geometry Series by label, a dict of Index by position)
    pp = gpd.sjoin(gpd.GeoDataFrame({'pid': parts.index.values},
                                    geometry=parts.geometry.representative_point().values, crs=crs),
                   gpd.GeoDataFrame({'parent': parents.index.values, 'parea': parents.area.values},
                                    geometry=parents.geometry.values, crs=crs),
                   predicate='within', how='inner').sort_values('parea').drop_duplicates('pid', keep='last')
    part_parent = pp.set_index('pid')['parent']
    # Parents mostly covered by their parts are drawn by the parts only
    drop = []
    for parent, pids in pp.groupby('parent')['pid']:
        g = parents.geometry.loc[parent]
        cover = parts.geometry.loc[pids.values].union_all().intersection(g).area if hasattr(parts.geometry, 'union_all') \
            else parts.geometry.loc[pids.values].unary_union.intersection(g).area
        if cover >= PARTS_COVER * g.area:
            drop.append(parent)

    # max_trips per parent: own osm_id in the poster's buildings file, else the footprint it lies in
    f_idx = freq.set_index('osm_id')
    trips = parents['osm_id'].map(f_idx['max_trips'])
    big = parents['osm_id'].map(f_idx['big'])
    miss = trips.isna()
    if miss.any():
        pts = gpd.GeoDataFrame(geometry=parents.loc[miss].geometry.representative_point(), crs=crs)
        j = gpd.sjoin(pts, freq[['max_trips', 'big', 'geometry']], predicate='within', how='left')
        j = j.groupby(level=0).agg({'max_trips': 'max', 'big': 'max'})
        trips[miss], big[miss] = j['max_trips'], j['big']
    # parts: the parent's value; orphans: own id, else location
    p_trips = part_parent.map(trips).reindex(parts.index)
    p_big = part_parent.map(big).reindex(parts.index)
    orphan = p_trips.isna()
    if orphan.any():
        p_trips[orphan] = parts.loc[orphan, 'osm_id'].map(f_idx['max_trips'])
        p_big[orphan] = parts.loc[orphan, 'osm_id'].map(f_idx['big'])
        still = p_trips.isna()
        if still.any():
            pts = gpd.GeoDataFrame(geometry=parts.loc[still].geometry.representative_point(), crs=crs)
            j = gpd.sjoin(pts, freq[['max_trips', 'big', 'geometry']], predicate='within', how='left')
            j = j.groupby(level=0).agg({'max_trips': 'max', 'big': 'max'})
            p_trips[still], p_big[still] = j['max_trips'], j['big']
    parents = parents.assign(trips=trips, big=big).drop(index=drop)
    parts = parts.assign(trips=p_trips, big=p_big)
    b = pd.concat([parents, parts])
    print(f"  {len(parents):,} buildings + {len(parts):,} parts ({len(drop):,} buildings drawn by their parts, "
          f"{orphan.sum():,} parts with no parent or an uncoloured one); no frequency: {b['trips'].isna().sum():,}")
    b['trips'] = b['trips'].fillna(0).astype(int)
    b['big'] = b['big'].fillna(0).astype(bool)
    if args.context_buildings:  # neutral silhouettes (e.g. New Jersey): own heights, no frequency
        ctx = read(Path(args.context_buildings), crs, bbox_m)
        ctx = ctx[ctx.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])]
        ctx = ctx[ctx.geometry.representative_point().within(crop)
                  & ~ctx['osm_id'].astype('int64').isin(b['osm_id'])]  # extracts overlap at the edges
        num_c = lambda c: pd.to_numeric(ctx[c], errors='coerce') if c in ctx else pd.Series(np.nan, ctx.index)  # noqa: E731
        ctx = ctx.assign(z1=num_c('height').fillna(num_c('building_levels') * LEVEL_M).fillna(DEFAULT_M),
                         z0=num_c('min_height').fillna(0.0), trips=-1, big=False)
        ctx = ctx[ctx['building_part'].isna()] if 'building_part' in ctx else ctx
        b = pd.concat([b, ctx[['osm_id', 'z0', 'z1', 'trips', 'big', 'geometry']]], ignore_index=True)
        print(f"  context silhouettes: {len(ctx):,}")
    if args.detail_bounds:  # far background: simpler outlines
        d0, d1, d2, d3 = (float(v) for v in args.detail_bounds.split(','))
        inner = box(cx + d0, cy + d1, cx + d2, cy + d3)
        far = ~b.geometry.representative_point().within(inner)
        b.loc[far, 'geometry'] = b.loc[far].geometry.simplify(FAR_SIMPLIFY_M)
        print(f"  far background: {far.sum():,} outlines simplified by {FAR_SIMPLIFY_M:g} m")

    col = poster_colours(city['key'])
    classes = col['classes']
    palette = [col['uncovered']] + [c for _, _, c in classes]
    context_class = len(palette)
    palette.append(list(CONTEXT_RGB))
    uppers = np.array([hi for _, hi, _ in classes])
    lowers = np.array([lo for lo, _, _ in classes])
    t = b['trips'].to_numpy()
    cls = np.searchsorted(uppers, t, side='left') + 1          # lower < t <= upper
    cls = np.where((t > lowers[0]) & ~(args.big_grey & b['big'].to_numpy()), np.minimum(cls, len(classes)), 0)
    cls = np.where(t < 0, context_class, cls)

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

    # 2D coverage tint: the poster's coverage classes (composited colour + alpha), per class
    cov = read(data / 'coverage_map_gates.gpkg', crs, bbox_m)
    attr = 'deduped_trips' if 'deduped_trips' in cov else 'max_trips'
    # Pixel-sized holes of the barrier-aware raster (the 2D poster blurs them away; in 3D they show
    # as dark shards): enclosed gaps < SMALL_GAP_M2 take the highest neighbouring value. Larger
    # gaps (fenced sites) stay.
    import shapely
    u = shapely.union_all(cov.geometry.values)
    holes = [Polygon(r) for q in (u.geoms if hasattr(u, 'geoms') else [u]) for r in q.interiors
             if Polygon(r).area < SMALL_GAP_M2]
    if holes:
        hg = gpd.GeoDataFrame(geometry=holes, crs=crs)
        j = gpd.sjoin(gpd.GeoDataFrame(geometry=hg.buffer(0.5), crs=crs), cov[[attr, 'geometry']],
                      predicate='intersects').groupby(level=0)[attr].max()
        cov = pd.concat([cov[[attr, 'geometry']], hg.assign(**{attr: j}).dropna(subset=[attr])], ignore_index=True)
        print(f"  coverage: {len(j):,} small gaps filled")
    cov_classes = col['coverage_classes']
    cup = np.array([hi for _, hi, _, _ in cov_classes])
    ck = np.searchsorted(cup, cov[attr].to_numpy(), side='left')
    cov = cov.assign(k=ck)[(cov[attr].to_numpy() > cov_classes[0][0]) & (ck < len(cov_classes))]
    coverage_palette = []
    for k, (lo, hi, rgb, alpha) in enumerate(cov_classes):
        sub = cov[cov['k'] == k]
        if len(sub):
            ground[f'coverage_{k}'] = ground_polys(sub, crop, cx, cy, tile=500.0, simplify=0)
        coverage_palette.append([*rgb, round(alpha, 4)])

    out_dir = HERE / 'scenes' / args.name
    out_dir.mkdir(parents=True, exist_ok=True)
    scene = {'city': city['key'], 'crs': crs, 'center_lonlat': [lon, lat], 'center_xy': [cx, cy],
             'radius': r, 'bbox_local': [round(v, 1) for v in (bbox_m[0] - cx, bbox_m[1] - cy, bbox_m[2] - cx, bbox_m[3] - cy)], 'shape': args.shape, 'data': str(data),
             'palette': palette, 'context_class': context_class, 'class_labels': ['0'] + [f'{lo:g}-{hi:g}' for lo, hi, _ in classes] + ['context'],
             'colours': {k: v for k, v in col.items() if k not in ('classes', 'coverage_classes')},
             'coverage_palette': coverage_palette, 'buildings': buildings, 'ground': ground}
    out = out_dir / 'scene.json'
    out.write_text(json.dumps(scene, separators=(',', ':')))
    print(f"  wrote {out} ({out.stat().st_size / 1e6:.1f} MB): {len(buildings):,} building polygons, "
          + ', '.join(f'{k} {len(v)}' for k, v in ground.items() if not k.startswith('coverage_'))
          + f", coverage {sum(len(v) for k, v in ground.items() if k.startswith('coverage_'))}")
    print('  class counts: ' + ' '.join(f'{k}:{n}' for k, n in zip(*np.unique(cls, return_counts=True))))


if __name__ == '__main__':
    main()
