"""
Standalone re-implementation of the QGIS print layouts in transit-frequency-map.qgz.

Reads the project XML (no QGIS needed) and reproduces:
  - the city's layer stack (visible layers of the 'Tlo/<group>' tree, in tree order)
  - symbology: single / categorized / graduated renderers, SimpleFill + SimpleLine
    symbol layers, symbol alpha, layer opacity, layer blend modes (Color Burn on
    buildings, etc.), blur draw effects (parks/forests), subset filters
  - the layout: page size, map item extent/position, frame, title + attribution
    labels (Noto Sans Condensed), the "Odjazdy/doba" legend

Usage:
    python render_qgis_style.py --city warsaw --coverage ../_data/warsaw/2026_10_04/coverage_map.gpkg \\
        --date 07.10.2026 --out ../png/how_many_rides_in_5_mins.png [--dpi 300]

    --dpi defaults to the layout's printResolution (300). Use e.g. 100 for previews.
    For a 1:1 QGIS export use export_layout.py instead (needs QGIS installed).
"""
import argparse
import pickle
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
import shapely
import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import font_manager
from matplotlib.collections import LineCollection
from matplotlib.patches import PathPatch, Rectangle
from matplotlib.path import Path as MplPath
from pyproj import Transformer
from scipy.ndimage import gaussian_filter
from shapely.geometry.polygon import orient

from cities import get_city, add_city_argument, PROJECT_DIR

PROJECT_FILE = PROJECT_DIR / "transit-frequency-map.qgz"
MM_TO_PT = 72 / 25.4
RESIDENTS_ALPHA = 0.3          # residents-only zones: same class colour, this much opacity
RESIDENTS_LABEL = 'tylko dla mieszkańców'
LEGEND_PAGE_MARGIN_MM = 5.15   # same inset as the map frames in the layouts
GREEN_LAYERS = {'parks', 'forests', 'meadow', 'grass', 'leisure', 'leisure_relations',
                'cemeteries', 'allotments'}

# Building colouring (--buildings, from 05_building_values.py)
COVERAGE_FADE = 0.6            # area fill opacity factor when buildings carry the colour
BUILDING_SHADE = 0.85          # class colour slightly darkened on buildings
BUILDING_SATURATION = 1.0      # saturation multiplier for building colours
BIG_BUILDING_RGB = '190,190,190,255'  # opaque grey: hides indoor corridors of malls etc.
BIG_BUILDING_RIM_M = 0         # >0: covered big buildings show an accessible rim this deep


def building_renderer(coverage_renderer, shade=BUILDING_SHADE, saturation=BUILDING_SATURATION):
    """Copy of the coverage renderer on max_trips with opaque, slightly darker fills."""
    import copy
    r = copy.deepcopy(coverage_renderer)
    r['attr'] = 'max_trips'
    r['blur_mm'] = 0.0
    for sym in r['symbols'].values():
        sym['alpha'] = 1.0
        for cls, props in sym['layers']:
            if cls == 'SimpleFill':
                import colorsys
                c = [int(v) / 255 for v in props['color'].split(',')[:3]]
                h, sat, val = colorsys.rgb_to_hsv(*c)
                c = colorsys.hsv_to_rgb(h, min(1.0, sat * saturation), val * shade)
                props['color'] = ','.join(str(int(round(v * 255))) for v in c) + ',255'
                props['outline_style'] = 'no'
    return r


def big_building_renderer(rgb=BIG_BUILDING_RGB):
    if rgb.count(',') == 2:
        rgb += ',255'
    return {'type': 'singleSymbol', 'attr': None, 'blur_mm': 0.0,
            'symbols': {'0': {'type': 'fill', 'alpha': 1.0, 'layers': [
                ('SimpleFill', {'color': rgb, 'style': 'solid',
                                'outline_style': 'no'})]}}}


# Alternative palettes for the frequency classes: (matplotlib colormap, start, end).
# Only the RGB changes; each class keeps its alpha from the QGIS style.
PALETTES = {
    'turbo': ('turbo', 0.0, 1.0),
    'turbo_light': ('turbo', 0.3, 1.0),   # skip turbo's dark blue start: low = light green
    'jet': ('jet', 0.0, 1.0),
    'rainbow': ('rainbow', 0.0, 1.0),
    'nipy_spectral': ('nipy_spectral', 0.08, 0.95),
    'gist_rainbow': ('gist_rainbow_r', 0.0, 1.0),  # magenta (low) -> red (high)
    'spectral': ('Spectral_r', 0.0, 1.0),          # blue (low) -> red (high)
    'plasma': ('plasma', 0.0, 0.95),
    'gnuplot': ('gnuplot', 0.15, 1.0),
    # Dark-rich scales, oriented light (low frequency) -> dark (high), like the QGIS ramp
    'magma': ('magma_r', 0.02, 0.97),
    'inferno': ('inferno_r', 0.02, 0.97),
    'rocket': ('rocket_r', 0.0, 0.97),        # seaborn
    'mako': ('mako_r', 0.0, 0.97),            # seaborn
    'cmrmap': ('CMRmap_r', 0.08, 0.98),
    'gnuplot2': ('gnuplot2_r', 0.10, 0.98),
    'fire': ('cet_fire_r', 0.05, 0.97),       # colorcet
    'bmy': ('cet_bmy_r', 0.0, 1.0),           # colorcet
    'kbc': ('cet_kbc_r', 0.05, 0.97),         # colorcet
    # CMasher (vivid, perceptually even), light -> dark
    'chroma': ('cmr.chroma_r', 0.04, 0.95),
    'rainforest': ('cmr.rainforest_r', 0.04, 0.95),
    'torch': ('cmr.torch_r', 0.04, 0.95),
    'ember': ('cmr.ember_r', 0.04, 0.95),
    'sunburst': ('cmr.sunburst_r', 0.04, 0.95),
    'flamingo': ('cmr.flamingo_r', 0.04, 0.95),
    'voltage': ('cmr.voltage_r', 0.04, 0.95),
    'neon': ('cmr.neon_r', 0.04, 0.95),
    'tropical': ('cmr.tropical_r', 0.04, 0.95),
}


def apply_palette(renderer, name):
    """Recolour a graduated renderer's classes in place with a matplotlib colormap."""
    if not name or name == 'qgis' or renderer['type'] != 'graduatedSymbol':
        return
    cmap_name, a, b = PALETTES[name]
    try:  # register extra colormaps (rocket/mako, cet_*) if those libraries are installed
        import seaborn  # noqa: F401
        import colorcet  # noqa: F401
    except ImportError:
        pass
    try:
        import cmasher  # noqa: F401  (cmr.* colormaps)
    except ImportError:
        pass
    cmap = matplotlib.colormaps[cmap_name] if hasattr(matplotlib, 'colormaps') \
        else plt.get_cmap(cmap_name)
    syms = [s for _, _, s, _, _ in renderer['ranges']]
    for k, sym_name in enumerate(syms):
        r, g, bl, _ = cmap(a + (b - a) * k / max(1, len(syms) - 1))
        for cls, props in renderer['symbols'][sym_name]['layers']:
            if cls == 'SimpleFill':
                alpha = props['color'].split(',')[3]
                props['color'] = f"{int(r * 255)},{int(g * 255)},{int(bl * 255)},{alpha}"
MM_PER_INCH = 25.4
POLY_BATCH = 3000

# QgsPainting::BlendMode
BLEND_NAMES = {0: 'normal', 1: 'lighten', 2: 'screen', 3: 'dodge', 4: 'addition',
               5: 'darken', 6: 'multiply', 7: 'burn', 8: 'overlay'}


# ---------------------------------------------------------------------------
# Fonts
# ---------------------------------------------------------------------------

def register_fonts():
    """Make Noto Sans Condensed (user-installed on Windows) visible to matplotlib."""
    import os
    dirs = [Path(os.environ.get('LOCALAPPDATA', '')) / 'Microsoft/Windows/Fonts',
            Path('C:/Windows/Fonts')]
    for d in dirs:
        for f in d.glob('NotoSans_Condensed-*.ttf'):
            font_manager.fontManager.addfont(str(f))


def font_props(family, size_pt, style='Regular'):
    weight = {'Medium': 500, 'Bold': 700, 'SemiBold': 600}.get(style, 400)
    return font_manager.FontProperties(family=family, size=size_pt, weight=weight,
                                       stretch='condensed')


# ---------------------------------------------------------------------------
# QGIS project parsing
# ---------------------------------------------------------------------------

def parse_color(s):
    """'r,g,b,a' (optionally followed by ',rgb:...' in newer QGIS) -> RGBA floats 0-1."""
    parts = s.split(',')[:4]
    return tuple(int(float(p)) / 255 for p in parts)


def symbol_layer_props(sl):
    props = {}
    for o in sl.iter('Option'):
        if o.get('name') and o.get('value') is not None:
            props[o.get('name')] = o.get('value')
    for p in sl.findall('prop'):
        props[p.get('k')] = p.get('v')
    return props


def parse_symbol(sym_el):
    return {
        'type': sym_el.get('type'),
        'alpha': float(sym_el.get('alpha', 1)),
        'layers': [(sl.get('class'), symbol_layer_props(sl))
                   for sl in sym_el.findall('layer') if sl.get('enabled', '1') == '1'],
    }


def parse_renderer(rd):
    symbols = {s.get('name'): parse_symbol(s) for s in rd.find('symbols')}
    r = {'type': rd.get('type'), 'attr': rd.get('attr'), 'symbols': symbols}
    if r['type'] == 'categorizedSymbol':
        r['categories'] = [(c.get('value'), c.get('symbol'), c.get('render') != 'false',
                            c.get('label')) for c in rd.iter('category')]
    elif r['type'] == 'graduatedSymbol':
        r['ranges'] = [(float(g.get('lower')), float(g.get('upper')), g.get('symbol'),
                        g.get('render') != 'false', g.get('label')) for g in rd.iter('range')]
    elif r['type'] != 'singleSymbol':
        raise NotImplementedError(f"Renderer {r['type']} not supported")
    # Draw effects (blur) on the renderer
    r['blur_mm'] = 0.0
    for eff in rd.iter('effect'):
        if eff.get('type') == 'blur':
            o = symbol_layer_props(eff)
            if o.get('enabled') == '1':
                r['blur_mm'] = float(o.get('blur_level', 0))
    return r


def parse_datasource(ds, project_dir):
    parts = ds.split('|')
    path = Path(parts[0])
    if not path.is_absolute():
        path = (project_dir / path).resolve()
    opts = {}
    for p in parts[1:]:
        k, _, v = p.partition('=')
        opts[k] = v
    return path, opts


def parse_layer(ml, project_dir):
    path, opts = parse_datasource(ml.find('datasource').text, project_dir)
    authid = ml.find('srs/spatialrefsys/authid')
    blend = ml.find('blendMode')
    opacity = ml.find('layerOpacity')
    return {
        'name': ml.find('layername').text,
        'path': path,
        'layername': opts.get('layername'),
        'subset': opts.get('subset'),
        'geometrytype': opts.get('geometrytype'),
        'crs': authid.text if authid is not None and authid.text else 'EPSG:4326',
        'opacity': float(opacity.text) if opacity is not None else 1.0,
        'blend': int(blend.text) if blend is not None else 0,
        'renderer': parse_renderer(ml.find('renderer-v2')),
    }


def load_project(project_file):
    with zipfile.ZipFile(project_file) as z:
        qgs = [n for n in z.namelist() if n.endswith('.qgs')][0]
        return ET.fromstring(z.read(qgs))


def visible_layer_ids(group_el):
    """Checked layers of a layer-tree group, top-most first (nested checked groups included)."""
    ids = []
    for c in group_el:
        if c.get('checked') != 'Qt::Checked':
            continue
        if c.tag == 'layer-tree-group':
            ids.extend(visible_layer_ids(c))
        elif c.tag == 'layer-tree-layer':
            ids.append(c.get('id'))
    return ids


def find_group(el, name):
    for g in el.iter('layer-tree-group'):
        if g.get('name') == name:
            return g
    raise KeyError(f"Layer-tree group {name} not found")


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def subset_to_query(subset):
    """Translate the simple OGR SQL subsets used in the project into a pandas query."""
    q = subset.strip()
    q = re.sub(r'"(\w+)"', r'`\1`', q)
    q = re.sub(r"\bNOT IN\b", 'not in', q, flags=re.I)
    q = re.sub(r"\bIN\b", 'in', q)
    q = re.sub(r"\bLIKE\s+'([^'%_]*)'", r"== '\1'", q, flags=re.I)  # LIKE without wildcards
    q = re.sub(r"(?<![=!<>])=(?!=)", '==', q)
    return q


_CACHE = {}


def load_layer_data(layer, extent, map_crs):
    key = (str(layer['path']), layer['layername'], layer['subset'], layer['geometrytype'])
    if key not in _CACHE:
        _CACHE.clear()  # keep at most one dataset in memory (consecutive duplicates only)
        _CACHE[key] = _load_layer_data(layer, extent, map_crs)
    return _CACHE[key]


CACHE_DIR = PROJECT_DIR / 'cache' / 'render'


def _cache_file(path, *parts):
    """Cache file for a source dataset; the key includes its size and mtime."""
    import hashlib
    st = Path(path).stat()
    key = '|'.join(str(p) for p in (Path(path).resolve(), st.st_size, int(st.st_mtime), *parts))
    return CACHE_DIR / f"{Path(path).stem}_{hashlib.md5(key.encode()).hexdigest()[:12]}.pkl"


def read_cached(path, map_crs, layername=None, prepare=None, label=''):
    """Whole dataset in map_crs, from the disk cache when the source is unchanged."""
    f = _cache_file(path, layername, map_crs, label)
    if f.exists():
        with open(f, 'rb') as fh:
            return pickle.load(fh)
    gdf = gpd.read_file(path, layer=layername)
    if prepare is not None:
        gdf = prepare(gdf)
    if not gdf.empty:
        gdf = gdf.to_crs(map_crs)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(f, 'wb') as fh:
        pickle.dump(gdf, fh, protocol=pickle.HIGHEST_PROTOCOL)
    return gdf


def _load_layer_data(layer, extent, map_crs):
    def prepare(gdf):
        if gdf.empty:
            return gdf
        if layer['subset']:
            gdf = gdf.query(subset_to_query(layer['subset']))
        if layer['geometrytype']:
            base = layer['geometrytype'].replace('Multi', '')
            gdf = gdf[gdf.geom_type.str.replace('Multi', '') == base]
        if gdf.crs is None:
            gdf = gdf.set_crs(layer['crs'])
        return gdf

    gdf = read_cached(layer['path'], map_crs, layer['layername'], prepare,
                      label=f"{layer['subset']}|{layer['geometrytype']}")
    if gdf.empty:
        return gdf
    x0, y0, x1, y1 = extent
    return gdf.cx[x0:x1, y0:y1]


def _load_layer_data_uncached(layer, extent, map_crs):
    to_layer = Transformer.from_crs(map_crs, layer['crs'], always_xy=True)
    bbox = to_layer.transform_bounds(*extent)
    gdf = gpd.read_file(layer['path'], layer=layer['layername'], bbox=bbox)
    if gdf.empty:
        return gdf
    if layer['subset']:
        gdf = gdf.query(subset_to_query(layer['subset']))
    if layer['geometrytype']:
        base = layer['geometrytype'].replace('Multi', '')
        gdf = gdf[gdf.geom_type.str.replace('Multi', '') == base]
    if gdf.crs is None:
        gdf = gdf.set_crs(layer['crs'])
    return gdf.to_crs(map_crs)


def assign_symbols(gdf, renderer):
    """Series of symbol names per feature (None = not drawn)."""
    t = renderer['type']
    if t == 'singleSymbol':
        return np.full(len(gdf), '0', dtype=object)
    vals = gdf[renderer['attr']]
    out = np.full(len(gdf), None, dtype=object)
    if t == 'categorizedSymbol':
        cats = {v: (s, r) for v, s, r, _ in renderer['categories']}
        catch_all = cats.get('')  # '' = all other values
        for i, v in enumerate(vals.astype(str).where(vals.notna(), '')):
            s, r = cats.get(v, catch_all or (None, False))
            out[i] = s if r else None
    else:
        v = vals.to_numpy(dtype=float)
        for k, (lo, hi, s, r, _) in enumerate(renderer['ranges']):
            m = (v > lo) & (v <= hi) if k else (v >= lo) & (v <= hi)
            if r:
                out[m & (out == None)] = s  # noqa: E711
    return out


# ---------------------------------------------------------------------------
# Drawing
# ---------------------------------------------------------------------------

def polygon_paths(geoms):
    """Yield compound matplotlib Paths (batches of polygons, holes respected)."""
    verts, codes, n = [], [], 0
    for g in geoms:
        polys = g.geoms if g.geom_type == 'MultiPolygon' else [g]
        for p in polys:
            if p.is_empty:
                continue
            p = orient(p, 1.0)
            for ring in [p.exterior, *p.interiors]:
                c = np.asarray(ring.coords)[:, :2]
                if len(c) < 3:
                    continue
                verts.append(c)
                codes.append(np.r_[MplPath.MOVETO, np.full(len(c) - 2, MplPath.LINETO),
                                   MplPath.CLOSEPOLY])
            n += 1
        if n >= POLY_BATCH:
            yield MplPath(np.concatenate(verts), np.concatenate(codes))
            verts, codes, n = [], [], 0
    if verts:
        yield MplPath(np.concatenate(verts), np.concatenate(codes))


def line_segments(geoms):
    out = []
    for g in geoms:
        if g.geom_type in ('Polygon', 'MultiPolygon'):
            g = g.boundary
        parts = g.geoms if hasattr(g, 'geoms') else [g]
        out.extend(np.asarray(p.coords)[:, :2] for p in parts if not p.is_empty)
    return out


def with_alpha(rgba, alpha):
    return (rgba[0], rgba[1], rgba[2], rgba[3] * alpha)


CAPSTYLE = {'square': 'projecting', 'flat': 'butt', 'round': 'round'}


def draw_symbol(ax, geoms, symbol, px_scale):
    """Draw geometries with a QGIS symbol (SimpleFill / SimpleLine layers)."""
    alpha = symbol['alpha']
    for cls, p in symbol['layers']:
        if cls == 'SimpleFill':
            fill = p.get('style', 'solid') != 'no'
            outline = p.get('outline_style', 'solid') != 'no'
            if not fill and not outline:
                continue
            face = with_alpha(parse_color(p['color']), alpha) if fill else 'none'
            edge = with_alpha(parse_color(p['outline_color']), alpha) if outline else 'none'
            lw = float(p.get('outline_width', 0.26)) * MM_TO_PT * px_scale if outline else 0
            for path in polygon_paths(geoms):
                ax.add_patch(PathPatch(path, facecolor=face, edgecolor=edge, linewidth=lw,
                                       joinstyle=p.get('joinstyle', 'bevel'),
                                       antialiased=True))
        elif cls == 'SimpleLine':
            if p.get('line_style', 'solid') == 'no':
                continue
            lc = LineCollection(
                line_segments(geoms),
                colors=[with_alpha(parse_color(p['line_color']), alpha)],
                linewidths=float(p.get('line_width', 0.26)) * MM_TO_PT * px_scale,
                capstyle=CAPSTYLE.get(p.get('capstyle', 'square'), 'projecting'),
                joinstyle=p.get('joinstyle', 'bevel'),
                antialiaseds=True)
            ax.add_collection(lc)
        else:
            print(f"    (symbol layer {cls} not supported, skipped)")


SUPERSAMPLE = 2    # rasterize at 2x and average down: smooth (anti-aliased) edges
ENGINE = 'rasterio'


def _composite_over(layer, frac, rgba):
    """Straight-alpha 'over' of a solid colour with per-pixel coverage frac onto layer (H,W,4)."""
    a = frac * rgba[3]
    if not a.any():
        return
    A = layer[..., 3]
    out_a = a + A * (1 - a)
    nz = out_a > 0
    for c in range(3):
        layer[..., c][nz] = (rgba[c] * a[nz] + layer[..., c][nz] * A[nz] * (1 - a[nz])) / out_a[nz]
    layer[..., 3] = out_a


def render_layer_rgba(gdf, renderer, extent, size_px, dpi):
    """Render one layer -> float32 RGBA (H, W, 4), straight alpha (rasterio engine)."""
    if ENGINE == 'matplotlib':
        return render_layer_rgba_mpl(gdf, renderer, extent, size_px, dpi)
    from affine import Affine
    from rasterio.features import rasterize
    w, h = size_px
    ss = SUPERSAMPLE
    W, H = w * ss, h * ss
    x0, y0, x1, y1 = extent
    transform = Affine((x1 - x0) / W, 0, x0, 0, -(y1 - y0) / H, y1)
    m_per_mm = (x1 - x0) / W * (dpi * ss / MM_PER_INCH)  # map metres per printed mm
    layer = np.zeros((h, w, 4), dtype=np.float32)

    def coverage(geoms):
        geoms = [g for g in geoms if g is not None and not g.is_empty]
        if not geoms:
            return None
        mask = rasterize(((g, 1) for g in geoms), out_shape=(H, W), transform=transform,
                         dtype='uint8')
        return mask.reshape(h, ss, w, ss).mean(axis=(1, 3), dtype=np.float32)

    syms = assign_symbols(gdf, renderer)
    if renderer['type'] == 'categorizedSymbol':
        order = [s for _, s, _, _ in renderer['categories']]
    elif renderer['type'] == 'graduatedSymbol':
        order = [s for _, _, s, _, _ in renderer['ranges']]
    else:
        order = ['0']
    geoms_all = gdf.geometry.to_numpy()
    is_poly = np.isin(gdf.geom_type.to_numpy(), ['Polygon', 'MultiPolygon'])
    for sname in order:
        m = syms == sname
        if not m.any():
            continue
        sym = renderer['symbols'][sname]
        geoms, polys = geoms_all[m], geoms_all[m & is_poly]
        for cls, p in sym['layers']:
            if cls == 'SimpleFill':
                if p.get('style', 'solid') != 'no' and len(polys):
                    f = coverage(polys)
                    if f is not None:
                        _composite_over(layer, f, with_alpha(parse_color(p['color']), sym['alpha']))
                if p.get('outline_style', 'solid') != 'no' and len(polys):
                    half = float(p.get('outline_width', 0.26)) * m_per_mm / 2
                    f = coverage(shapely.buffer(shapely.boundary(polys), half))
                    if f is not None:
                        _composite_over(layer, f, with_alpha(parse_color(p['outline_color']), sym['alpha']))
            elif cls == 'SimpleLine':
                if p.get('line_style', 'solid') == 'no':
                    continue
                lines = np.where(np.isin(gdf.geom_type.to_numpy()[m], ['Polygon', 'MultiPolygon']),
                                 shapely.boundary(geoms), geoms)
                half = float(p.get('line_width', 0.26)) * m_per_mm / 2
                cap = {'square': 'square', 'flat': 'flat', 'round': 'round'}.get(p.get('capstyle'), 'square')
                f = coverage(shapely.buffer(lines, half, cap_style=cap, join_style='bevel'))
                if f is not None:
                    _composite_over(layer, f, with_alpha(parse_color(p['line_color']), sym['alpha']))
    return layer


def road_edges_rgba(gdf, renderer, extent, size_px, dpi, edge_mm, rgb=(1.0, 1.0, 1.0)):
    """Thin opaque outline along both edges of each drawn road (at its symbol width)."""
    from affine import Affine
    from rasterio.features import rasterize
    w, h = size_px
    ss = SUPERSAMPLE
    W, H = w * ss, h * ss
    x0, y0, x1, y1 = extent
    transform = Affine((x1 - x0) / W, 0, x0, 0, -(y1 - y0) / H, y1)
    m_per_mm = (x1 - x0) / W * (dpi * ss / MM_PER_INCH)
    layer = np.zeros((h, w, 4), dtype=np.float32)
    syms = assign_symbols(gdf, renderer)
    geoms_all = gdf.geometry.to_numpy()
    edges = []
    for sname in set(s for s in syms if s is not None):
        m = syms == sname
        for cls, p in renderer['symbols'][sname]['layers']:
            if cls != 'SimpleLine' or p.get('line_style', 'solid') == 'no':
                continue
            half = float(p.get('line_width', 0.26)) * m_per_mm / 2
            road = shapely.buffer(geoms_all[m], half, cap_style='flat', join_style='bevel')
            edges.append(shapely.buffer(shapely.boundary(road), edge_mm * m_per_mm / 2))
    edges = [g for arr in edges for g in arr if g is not None and not g.is_empty]
    if not edges:
        return layer
    mask = rasterize(((g, 1) for g in edges), out_shape=(H, W), transform=transform, dtype='uint8')
    frac = mask.reshape(h, ss, w, ss).mean(axis=(1, 3), dtype=np.float32)
    _composite_over(layer, frac, (*rgb, 1.0))
    return layer


def render_layer_rgba_mpl(gdf, renderer, extent, size_px, dpi):
    """Render one layer onto a transparent canvas -> float32 RGBA (H, W, 4), straight alpha."""
    w, h = size_px
    fig = plt.figure(figsize=(w / dpi, h / dpi), dpi=dpi)
    fig.patch.set_alpha(0)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_axis_off()
    ax.patch.set_alpha(0)
    ax.set_xlim(extent[0], extent[2])
    ax.set_ylim(extent[1], extent[3])

    syms = assign_symbols(gdf, renderer)
    # Draw in renderer order (categories/ranges list order; later ones on top)
    if renderer['type'] == 'categorizedSymbol':
        order = [s for _, s, _, _ in renderer['categories']]
    elif renderer['type'] == 'graduatedSymbol':
        order = [s for _, _, s, _, _ in renderer['ranges']]
    else:
        order = ['0']
    geoms = gdf.geometry.to_numpy()
    for s in order:
        m = syms == s
        if m.any():
            draw_symbol(ax, geoms[m], renderer['symbols'][s], 1.0)

    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba(), dtype=np.float32) / 255.0
    plt.close(fig)
    # Float rounding of the figure size can be off by a pixel: match the canvas exactly
    if rgba.shape[:2] != (h, w):
        fixed = np.zeros((h, w, 4), dtype=np.float32)
        hh, ww = min(h, rgba.shape[0]), min(w, rgba.shape[1])
        fixed[:hh, :ww] = rgba[:hh, :ww]
        rgba = fixed
    return rgba


def unpremultiply_blur(rgba, sigma):
    """Gaussian blur in premultiplied space (like QGIS's blur effect)."""
    a = rgba[..., 3:4]
    pm = rgba[..., :3] * a
    pm = np.stack([gaussian_filter(pm[..., i], sigma) for i in range(3)], axis=-1)
    a2 = gaussian_filter(a[..., 0], sigma)[..., None]
    rgb = np.divide(pm, a2, out=np.zeros_like(pm), where=a2 > 1e-6)
    return np.concatenate([rgb, a2], axis=-1)


def blend(dst, src, mode):
    """Composite straight-alpha src (H,W,4) onto opaque dst (H,W,3) in place."""
    cs, a = src[..., :3], src[..., 3:4]
    cb = dst
    if mode == 'normal':
        b = cs
    elif mode == 'burn':
        b = 1 - np.minimum(1, np.divide(1 - cb, cs, out=np.ones_like(cs), where=cs > 0))
        b = np.where(cs > 0, b, 0)
    elif mode == 'multiply':
        b = cb * cs
    elif mode == 'darken':
        b = np.minimum(cb, cs)
    elif mode == 'lighten':
        b = np.maximum(cb, cs)
    elif mode == 'screen':
        b = 1 - (1 - cb) * (1 - cs)
    elif mode == 'dodge':
        b = np.minimum(1, np.divide(cb, 1 - cs, out=np.ones_like(cs), where=cs < 1))
    elif mode == 'addition':
        b = np.minimum(1, cb + cs)
    elif mode == 'overlay':
        b = np.where(cb <= 0.5, 2 * cb * cs, 1 - 2 * (1 - cb) * (1 - cs))
    else:
        raise NotImplementedError(mode)
    dst[:] = cb * (1 - a) + b * a


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------

def parse_mm(s):
    return [float(v) for v in s.replace(',mm', '').split(',')]


def item_rect(item):
    """(x, y, w, h) in mm from the top-left of the page, honouring referencePoint."""
    x, y = parse_mm(item.get('position'))
    w, h = parse_mm(item.get('size'))
    ref = int(item.get('referencePoint', 0))
    x -= w * (ref % 3) / 2
    y -= h * (ref // 3) / 2
    return x, y, w, h


def label_font(item):
    ts = item.find('.//text-style')
    if ts is not None:
        return ts.get('fontFamily'), float(ts.get('fontSize')), ts.get('namedStyle', 'Regular')
    fd = item.find('LabelFont')
    desc = fd.get('description').split(',') if fd is not None else ['Noto Sans Condensed', '10']
    return desc[0], float(desc[1]), 'Regular'


def legend_style(item, name):
    st = [s for s in item.iter('style') if s.get('name') == name][0]
    desc = st.find('styleFont').get('description').split(',')
    style = desc[10] if len(desc) > 10 else 'Regular'
    return {'family': desc[0], 'size': float(desc[1]), 'style': style,
            'marginTop': float(st.get('marginTop', 0)), 'marginLeft': float(st.get('marginLeft', 0))}


def draw_legend(fig, page_mm, item, legend_layer, to_fig, residents=False, shift_mm=0.0,
                shift_y_mm=0.0):
    """Single-column legend: layer title + one patch per renderer class."""
    box = float(item.get('boxSpace', 2))
    sw, sh = float(item.get('symbolWidth', 7)), float(item.get('symbolHeight', 4))
    title_st = legend_style(item, 'subgroup')
    sym_st = legend_style(item, 'symbol')
    lab_st = legend_style(item, 'symbolLabel')
    r = legend_layer['renderer']
    entries = [(lab, r['symbols'][s]) for lo, hi, s, rend, lab in r['ranges']] \
        if r['type'] == 'graduatedSymbol' else \
        [(lab, r['symbols'][s]) for v, s, rend, lab in r['categories']]

    pt_to_mm = 25.4 / 72
    title_h = title_st['size'] * pt_to_mm
    row_h = sh + sym_st['marginTop']
    extra = [RESIDENTS_LABEL] if residents else []
    height = box + title_h + (len(entries) + len(extra)) * row_h + box
    lab_fp = font_props(lab_st['family'], lab_st['size'], lab_st['style'])
    # Width from the longest label
    renderer = fig.canvas.get_renderer()
    text_w = 0
    for lab in [e[0] for e in entries] + extra:
        t = fig.text(0, 0, lab, fontproperties=lab_fp)
        text_w = max(text_w, t.get_window_extent(renderer).width / fig.dpi * 25.4)
        t.remove()
    width = box + sw + lab_st['marginLeft'] + text_w + box

    x, y, w, h = item_rect(item)
    ref = int(item.get('referencePoint', 0))
    # Resize-to-contents keeps the reference point fixed
    ax_, ay_ = parse_mm(item.get('position'))
    ax_ += shift_mm
    ay_ += shift_y_mm
    x = ax_ - width * (ref % 3) / 2
    y = ay_ - height * (ref // 3) / 2
    # A longer label (e.g. the residents row) must not push the legend off the page
    x = max(LEGEND_PAGE_MARGIN_MM, min(x, page_mm[0] - LEGEND_PAGE_MARGIN_MM - width))

    frame_w = parse_mm(item.get('outlineWidthM', '0.3,mm'))[0] * MM_TO_PT
    fig.add_artist(Rectangle(to_fig(x, y + height), width / page_mm[0], height / page_mm[1],
                             transform=fig.transFigure, facecolor='white', edgecolor='black',
                             linewidth=frame_w, zorder=10))
    cy = y + box
    fig.text(*to_fig(x + box, cy), legend_layer['legend_title'], va='top', ha='left',
             fontproperties=font_props(title_st['family'], title_st['size'], title_st['style']),
             zorder=11)
    cy += title_h
    for lab, sym in entries:
        cy += sym_st['marginTop']
        cls, p = sym['layers'][0]
        color = with_alpha(parse_color(p['color']), sym['alpha'] * legend_layer['opacity'])
        fig.add_artist(Rectangle(to_fig(x + box, cy + sh), sw / page_mm[0], sh / page_mm[1],
                                 transform=fig.transFigure, facecolor=color,
                                 edgecolor='none', zorder=11))
        fig.text(*to_fig(x + box + sw + lab_st['marginLeft'], cy + sh / 2), lab,
                 va='center', ha='left', fontproperties=lab_fp, zorder=11)
        cy += sh
    if residents:
        # Pale swatch: a mid-range class at the residents-only opacity
        cls, p = entries[len(entries) // 2][1]['layers'][0]
        cy += sym_st['marginTop']
        color = with_alpha(parse_color(p['color']), RESIDENTS_ALPHA * legend_layer['opacity'])
        fig.add_artist(Rectangle(to_fig(x + box, cy + sh), sw / page_mm[0], sh / page_mm[1],
                                 transform=fig.transFigure, facecolor=color,
                                 edgecolor='none', zorder=11))
        fig.text(*to_fig(x + box + sw + lab_st['marginLeft'], cy + sh / 2), RESIDENTS_LABEL,
                 va='center', ha='left', fontproperties=lab_fp, zorder=11)


def main():
    parser = argparse.ArgumentParser(description='Render a city map in the QGIS layout style')
    add_city_argument(parser)
    parser.add_argument('--coverage', required=True, help='coverage_map*.gpkg to show')
    parser.add_argument('--date', required=True, help='Date for the title, DD.MM.YYYY')
    parser.add_argument('--out', required=True, help='Output PNG')
    parser.add_argument('--dpi', type=float, default=None, help='Default: layout print resolution')
    parser.add_argument('--palette', default='qgis', choices=['qgis'] + sorted(PALETTES),
                        help='Frequency class colours (default: as in the QGIS project)')
    parser.add_argument('--residents-legend', action='store_true',
                        help='Add a legend row for the pale residents-only colour')
    parser.add_argument('--residents', default=None,
                        help='coverage_map_*_residents.gpkg: drawn pale where the public '
                             'coverage does not reach (accessible to residents only)')
    parser.add_argument('--buildings', default=None,
                        help='buildings_<variant>.gpkg from 05_building_values.py: colour small '
                             'buildings by their best frequency, grey out big ones')
    parser.add_argument('--coverage-fade', type=float, default=COVERAGE_FADE,
                        help=f'With --buildings: area fill opacity factor (default {COVERAGE_FADE})')
    parser.add_argument('--building-shade', type=float, default=BUILDING_SHADE,
                        help=f'With --buildings: class colour multiplier on buildings (default {BUILDING_SHADE})')
    parser.add_argument('--building-saturation', type=float, default=BUILDING_SATURATION,
                        help=f'With --buildings: saturation multiplier (default {BUILDING_SATURATION})')
    parser.add_argument('--big-rim', type=float, default=BIG_BUILDING_RIM_M,
                        help='With --buildings: coloured rim depth (m) on big buildings; 0 = plain grey')
    parser.add_argument('--uncovered-rgb', default=None,
                        help="With --buildings: solid colour for all non-coloured buildings, e.g. "
                             "'64,64,64' (Schwarzplan look); default keeps the QGIS building style")
    parser.add_argument('--extend-left-m', type=float, default=0,
                        help='Widen the map to the left by this many metres (page grows, scale kept)')
    parser.add_argument('--extend-right-m', type=float, default=0,
                        help='Widen the map to the right by this many metres (page grows, scale kept)')
    parser.add_argument('--crop', default=None,
                        help="Quick preview of a window only: 'lon,lat,width_m,height_m' "
                             "(no title/legend; loads only data inside the window)")
    parser.add_argument('--engine', default='rasterio', choices=['rasterio', 'matplotlib'],
                        help='Layer drawing engine (rasterio: fast; matplotlib: original)')
    parser.add_argument('--restricted-buildings', action='store_true',
                        help='With --buildings: paint buildings behind fences/closed gates like any '
                             'other (value from the residents coverage); their ground stays unpainted')
    parser.add_argument('--green-rgb', default=None,
                        help="Recolour parks/forests/grass etc. (fill RGB, alpha kept), "
                             "e.g. '236,239,236' for a very light grey-green")
    parser.add_argument('--road-edge-mm', type=float, default=0,
                        help='Thin opaque white outline along road edges, width in mm (e.g. 0.1)')
    parser.add_argument('--project', default=str(PROJECT_FILE))
    args = parser.parse_args()

    city = get_city(args.city)
    global ENGINE
    ENGINE = args.engine
    register_fonts()
    root = load_project(Path(args.project))
    project_dir = Path(args.project).resolve().parent
    # Cities without their own QGIS layout borrow a template city's layout and styles
    tmpl = city if 'qgis_layout' in city else get_city(city.get('template', 'krakow'))
    if tmpl is not city:
        print(f"  no QGIS layout for {city['name']}: using {tmpl['name']} layout as template")
    layout = [l for l in root.iter('Layout') if l.get('name') == tmpl['qgis_layout']][0]
    dpi = args.dpi or float(layout.get('printResolution', 300))
    map_crs = 'EPSG:2180'
    for crs_el in root.iter('projectCrs'):
        a = crs_el.find('spatialrefsys/authid')
        if a is not None and a.text:
            map_crs = a.text
    if tmpl is not city:
        map_crs = city['crs_metric']

    items = {it.get('type'): [] for it in layout.iter('LayoutItem')}
    for it in layout.iter('LayoutItem'):
        items[it.get('type')].append(it)
    page = items['65638'][0]
    map_item = items['65639'][0]
    labels = items.get('65641', [])
    legend_item = items.get('65642', [None])[0]

    page_mm = parse_mm(page.get('size'))[:2]
    mx, my, mw, mh = item_rect(map_item)
    ext = map_item.find('Extent')
    extent = tuple(float(ext.get(k)) for k in ('xmin', 'ymin', 'xmax', 'ymax'))
    label_shift = legend_shift = 0.0
    label_dy = legend_dy = 0.0   # vertical shifts for bottom-anchored items (template mode)
    old_page_h = page_mm[1]
    if tmpl is not city:
        mm_per_m = mw / (extent[2] - extent[0])
        b = city['bbox']
        extent = Transformer.from_crs('EPSG:4326', map_crs, always_xy=True).transform_bounds(
            b['west'], b['south'], b['east'], b['north'])
        right, bottom = page_mm[0] - (mx + mw), page_mm[1] - (my + mh)
        old_w = page_mm[0]
        mw, mh = (extent[2] - extent[0]) * mm_per_m, (extent[3] - extent[1]) * mm_per_m
        page_mm = [mx + mw + right, my + mh + bottom]
        dw, dh = page_mm[0] - old_w, page_mm[1] - old_page_h
        label_shift, label_dy = dw / 2, dh
        if legend_item is not None:
            lx, ly = parse_mm(legend_item.get('position'))
            legend_shift = dw if lx > old_w / 2 else 0.0
            legend_dy = dh if ly > old_page_h / 2 else 0.0
        print(f"  template frame: {(extent[2]-extent[0])/1000:.1f} x {(extent[3]-extent[1])/1000:.1f} km, "
              f"page {page_mm[0]:.0f} x {page_mm[1]:.0f} mm")
    if args.extend_left_m or args.extend_right_m:
        mm_per_m = mw / (extent[2] - extent[0])
        dl, dr = args.extend_left_m * mm_per_m, args.extend_right_m * mm_per_m
        old_page_w = page_mm[0]
        extent = (extent[0] - args.extend_left_m, extent[1], extent[2] + args.extend_right_m, extent[3])
        mw += dl + dr
        page_mm[0] += dl + dr
        label_shift = (dl + dr) / 2  # centred labels stay centred
        if legend_item is not None and parse_mm(legend_item.get('position'))[0] > old_page_w / 2:
            legend_shift = dl + dr    # right-anchored legend follows the right edge
        print(f"  widened by {args.extend_left_m:g} m left / {args.extend_right_m:g} m right "
              f"(+{dl + dr:.1f} mm page width)")
    if args.crop:
        lon, lat, cw, ch = (float(v) for v in args.crop.split(','))
        cx, cy = Transformer.from_crs('EPSG:4326', map_crs, always_xy=True).transform(lon, lat)
        mm_per_m = mw / (extent[2] - extent[0])  # keep the layout's scale
        extent = (cx - cw / 2, cy - ch / 2, cx + cw / 2, cy + ch / 2)
        mx, my, mw, mh = 0.0, 0.0, cw * mm_per_m, ch * mm_per_m
        page_mm = [mw, mh]
        labels, legend_item = [], None
        print(f"  crop preview: {cw:g} x {ch:g} m around {lat:.4f}, {lon:.4f}")
    map_px = (int(round(mw / MM_PER_INCH * dpi)), int(round(mh / MM_PER_INCH * dpi)))
    print(f"Layout {city['qgis_layout']}: page {page_mm[0]}x{page_mm[1]} mm, "
          f"map {map_px[0]}x{map_px[1]} px at {dpi:g} dpi")

    # Layer stack
    layers_by_id = {ml.find('id').text: ml for ml in root.iter('maplayer')}
    tree = root.find('layer-tree-group')
    group = find_group(find_group(tree, 'Tlo'), tmpl['qgis_group'])
    stack = [parse_layer(layers_by_id[i], project_dir) for i in visible_layer_ids(group)]
    if tmpl is not city:
        kept = []
        for layer in stack:
            if 'coverage_map' in layer['path'].name:
                kept.append(layer)
                continue
            new_path = city['osm_dir'] / layer['path'].name
            if new_path.exists():
                kept.append(dict(layer, path=new_path))
            else:
                print(f"  (template layer {layer['name']}: {new_path.name} missing for {city['name']}, skipped)")
        stack = kept
    # Water always sits above the coverage colours (you can't walk on water);
    # stack is top-most first, so move water layers just in front of the coverage
    cov_pos = next((i for i, l in enumerate(stack) if 'coverage_map' in l['path'].name), None)
    if cov_pos is not None:
        water = [l for l in stack[cov_pos + 1:] if l['path'].stem == 'water']
        if water:
            stack = [l for l in stack if l not in water]
            cov_pos = next(i for i, l in enumerate(stack) if 'coverage_map' in l['path'].name)
            stack[cov_pos:cov_pos] = water
    if args.green_rgb:
        for layer in stack:
            if layer['path'].stem in GREEN_LAYERS:
                for sym in layer['renderer']['symbols'].values():
                    for cls, props in sym['layers']:
                        if cls == 'SimpleFill' and 'color' in props:
                            alpha = props['color'].split(',')[3]
                            props['color'] = f"{args.green_rgb},{alpha}"
    coverage = Path(args.coverage).resolve()
    for layer in stack:
        if 'coverage_map' in layer['path'].name:
            layer['path'], layer['layername'] = coverage, None  # single-layer gpkg; name follows the file
            apply_palette(layer['renderer'], args.palette)

    bld = None
    if args.buildings:
        bld = read_cached(args.buildings, map_crs)
        if not args.residents and not args.restricted_buildings:
            # Restricted areas switched off: residents-only buildings count as uncovered
            bld = bld[~bld['residents_only'] | bld['big']]
        styled_ids = set(bld['osm_id'].astype('int64'))
        print(f"  buildings: {(~bld['big']).sum():,} coloured, {bld['big'].sum():,} big (grey)")

    uncovered_done = False

    # Composite bottom-up
    bg = parse_color(','.join(map_item.find('BackgroundColor').get(k)
                              for k in ('red', 'green', 'blue', 'alpha')))
    canvas = np.empty((map_px[1], map_px[0], 3), dtype=np.float32)
    canvas[:] = np.array(bg[:3]) * bg[3] + (1 - bg[3])  # over white page
    for layer in reversed(stack):
        mode = BLEND_NAMES.get(layer['blend'], 'normal')
        print(f"  {layer['name']:<20} opacity={layer['opacity']:<5g} blend={mode:<7}", end=' ', flush=True)
        gdf = load_layer_data(layer, extent, map_crs)
        if bld is not None and layer['path'].name == 'buildings.gpkg' and 'osm_id' in gdf.columns:
            gdf = gdf[~gdf['osm_id'].astype('int64').isin(styled_ids)]  # styled separately
            if args.uncovered_rgb:
                if uncovered_done:
                    print("(skipped: Schwarzplan mode)")
                    continue
                uncovered_done = True
                layer = dict(layer, renderer=big_building_renderer(args.uncovered_rgb),
                             blend=0, opacity=1.0)
                mode = 'normal'
        print(f"{len(gdf):>8,} features", flush=True)
        if gdf.empty:
            continue
        rgba = render_layer_rgba(gdf, layer['renderer'], extent, map_px, dpi)
        if layer['renderer']['blur_mm'] > 0:
            radius_px = layer['renderer']['blur_mm'] / MM_PER_INCH * dpi
            rgba = unpremultiply_blur(rgba, radius_px / 2)
        rgba[..., 3] *= layer['opacity']
        if bld is not None and layer['path'] == coverage:
            rgba[..., 3] *= args.coverage_fade
        if args.residents and layer['path'] == coverage:
            # Residents-only zones: same symbology, pale, only where public coverage is absent
            res_layer = dict(layer, path=Path(args.residents).resolve(), layername=None)
            res = render_layer_rgba(load_layer_data(res_layer, extent, map_crs),
                                    layer['renderer'], extent, map_px, dpi)
            res[..., 3] *= layer['opacity'] * RESIDENTS_ALPHA * (rgba[..., 3] < 0.01)
            blend(canvas, res, 'normal')
            del res
        blend(canvas, rgba, mode)
        del rgba
        if args.road_edge_mm > 0 and layer['path'].stem == 'roads':
            blend(canvas, road_edges_rgba(gdf, layer['renderer'], extent, map_px, dpi,
                                          args.road_edge_mm), 'normal')
        if bld is not None and layer['path'] == coverage:
            # Buildings carry the colour: best frequency per small building, big ones grey
            br = building_renderer(layer['renderer'], args.building_shade, args.building_saturation)
            for subset, alpha in [(bld[~bld['big'] & ~bld['residents_only']], 1.0),
                                  (bld[~bld['big'] & bld['residents_only']],
                                   RESIDENTS_ALPHA + 0.15 if args.residents else 1.0)]:
                if len(subset):
                    b_rgba = render_layer_rgba(subset, br, extent, map_px, dpi)
                    b_rgba[..., 3] *= alpha
                    blend(canvas, b_rgba, 'normal')
                    del b_rgba
            big = bld[bld['big']]
            if len(big):
                # Base: the uncovered building colour
                b_rgba = render_layer_rgba(big, big_building_renderer(args.uncovered_rgb or BIG_BUILDING_RGB),
                                           extent, map_px, dpi)
                blend(canvas, b_rgba, 'normal')
                del b_rgba
                # The isochrones enter big buildings (10 m, plus indoor walkways) but never
                # cross them: show that actual coverage inside the footprint, building-strength
                bi, ci = gdf.sindex.query(big.geometry, predicate='intersects')
                if len(bi):
                    inside = gpd.GeoDataFrame(
                        {'max_trips': gdf['deduped_trips'].to_numpy()[ci]},
                        geometry=shapely.intersection(gdf.geometry.to_numpy()[ci], big.geometry.to_numpy()[bi]),
                        crs=gdf.crs)
                    inside = inside[~inside.geometry.is_empty]
                    b_rgba = render_layer_rgba(inside, br, extent, map_px, dpi)
                    blend(canvas, b_rgba, 'normal')
                    del b_rgba

    # Page
    fig = plt.figure(figsize=(page_mm[0] / MM_PER_INCH, page_mm[1] / MM_PER_INCH), dpi=dpi)
    fig.patch.set_facecolor('white')

    def to_fig(x_mm, y_mm):  # page mm (top-left origin) -> figure fraction
        return x_mm / page_mm[0], 1 - y_mm / page_mm[1]

    ax = fig.add_axes([mx / page_mm[0], 1 - (my + mh) / page_mm[1], mw / page_mm[0], mh / page_mm[1]])
    ax.imshow(np.clip(canvas, 0, 1), interpolation='nearest', aspect='auto')
    ax.set_xticks([])
    ax.set_yticks([])
    frame_w = parse_mm(map_item.get('outlineWidthM', '0.3,mm'))[0] * MM_TO_PT
    for s in ax.spines.values():
        s.set_linewidth(frame_w if map_item.get('frame') == 'true' else 0)

    for lab in labels:
        text = lab.get('labelText')
        if 'stan na' in text:
            text = re.sub(r'stan na [0-9.]+', f'stan na {args.date}', text)
        x, y, w, h = item_rect(lab)
        x += label_shift
        if y > old_page_h / 2:
            y += label_dy
        fam, size, style = label_font(lab)
        fig.text(*to_fig(x + w / 2, y + h / 2), text, ha='center', va='center',
                 fontproperties=font_props(fam, size, style))

    if legend_item is not None:
        leg_tree = legend_item.find('layer-tree-group')
        leg_node = [n for n in leg_tree.iter('layer-tree-layer')][0]
        leg_layer = parse_layer(layers_by_id[leg_node.get('id')], project_dir)
        leg_layer['legend_title'] = leg_node.get('name')
        apply_palette(leg_layer['renderer'], args.palette)
        draw_legend(fig, page_mm, legend_item, leg_layer, to_fig,
                    residents=bool(args.residents) and args.residents_legend, shift_mm=legend_shift,
                    shift_y_mm=legend_dy)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    # Save via a temp file; if the target is locked (open in a viewer / OneDrive sync),
    # keep the render under a timestamped name instead of failing
    import os
    from datetime import datetime as _dt
    tmp = out.with_name(out.stem + '.tmp' + out.suffix)
    fig.savefig(tmp, dpi=dpi, facecolor='white')
    try:
        os.replace(tmp, out)
    except OSError:
        alt = out.with_name(f"{out.stem}_{_dt.now():%H%M}{out.suffix}")
        os.replace(tmp, alt)
        print(f"  ({out.name} is locked - saved as {alt.name})")
        out = alt
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == '__main__':
    main()
