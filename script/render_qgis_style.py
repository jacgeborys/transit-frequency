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
import re
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path

import numpy as np
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

# Building colouring (--buildings, from 05_building_values.py)
COVERAGE_FADE = 0.6            # area fill opacity factor when buildings carry the colour
BUILDING_SHADE = 0.85          # class colour slightly darkened on buildings
BIG_BUILDING_RGB = '190,190,190,255'  # opaque grey: hides indoor corridors of malls etc.


def building_renderer(coverage_renderer):
    """Copy of the coverage renderer on max_trips with opaque, slightly darker fills."""
    import copy
    r = copy.deepcopy(coverage_renderer)
    r['attr'] = 'max_trips'
    r['blur_mm'] = 0.0
    for sym in r['symbols'].values():
        sym['alpha'] = 1.0
        for cls, props in sym['layers']:
            if cls == 'SimpleFill':
                c = [int(v) for v in props['color'].split(',')[:3]]
                props['color'] = ','.join(str(int(v * BUILDING_SHADE)) for v in c) + ',255'
                props['outline_style'] = 'no'
    return r


def big_building_renderer():
    return {'type': 'singleSymbol', 'attr': None, 'blur_mm': 0.0,
            'symbols': {'0': {'type': 'fill', 'alpha': 1.0, 'layers': [
                ('SimpleFill', {'color': BIG_BUILDING_RGB, 'style': 'solid',
                                'outline_style': 'no'})]}}}


# Alternative palettes for the frequency classes: (matplotlib colormap, start, end).
# Only the RGB changes; each class keeps its alpha from the QGIS style.
PALETTES = {
    'turbo': ('turbo', 0.0, 1.0),
    'turbo_light': ('turbo', 0.3, 1.0),   # skip turbo's dark blue start: low = light green
}


def apply_palette(renderer, name):
    """Recolour a graduated renderer's classes in place with a matplotlib colormap."""
    if not name or name == 'qgis' or renderer['type'] != 'graduatedSymbol':
        return
    cmap_name, a, b = PALETTES[name]
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


def _load_layer_data(layer, extent, map_crs):
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


def render_layer_rgba(gdf, renderer, extent, size_px, dpi):
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


def draw_legend(fig, page_mm, item, legend_layer, to_fig, residents=False):
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
    parser.add_argument('--project', default=str(PROJECT_FILE))
    args = parser.parse_args()

    city = get_city(args.city)
    register_fonts()
    root = load_project(Path(args.project))
    project_dir = Path(args.project).resolve().parent
    layout = [l for l in root.iter('Layout') if l.get('name') == city['qgis_layout']][0]
    dpi = args.dpi or float(layout.get('printResolution', 300))
    map_crs = 'EPSG:2180'
    for crs_el in root.iter('projectCrs'):
        a = crs_el.find('spatialrefsys/authid')
        if a is not None and a.text:
            map_crs = a.text

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
    map_px = (int(round(mw / MM_PER_INCH * dpi)), int(round(mh / MM_PER_INCH * dpi)))
    print(f"Layout {city['qgis_layout']}: page {page_mm[0]}x{page_mm[1]} mm, "
          f"map {map_px[0]}x{map_px[1]} px at {dpi:g} dpi")

    # Layer stack
    layers_by_id = {ml.find('id').text: ml for ml in root.iter('maplayer')}
    tree = root.find('layer-tree-group')
    group = find_group(find_group(tree, 'Tlo'), city['qgis_group'])
    stack = [parse_layer(layers_by_id[i], project_dir) for i in visible_layer_ids(group)]
    coverage = Path(args.coverage).resolve()
    for layer in stack:
        if 'coverage_map' in layer['path'].name:
            layer['path'], layer['layername'] = coverage, None  # single-layer gpkg; name follows the file
            apply_palette(layer['renderer'], args.palette)

    bld = None
    if args.buildings:
        bld = gpd.read_file(args.buildings).to_crs(map_crs)
        styled_ids = set(bld['osm_id'].astype('int64'))
        print(f"  buildings: {(~bld['big']).sum():,} coloured, {bld['big'].sum():,} big (grey)")

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
        print(f"{len(gdf):>8,} features", flush=True)
        if gdf.empty:
            continue
        rgba = render_layer_rgba(gdf, layer['renderer'], extent, map_px, dpi)
        if layer['renderer']['blur_mm'] > 0:
            radius_px = layer['renderer']['blur_mm'] / MM_PER_INCH * dpi
            rgba = unpremultiply_blur(rgba, radius_px / 2)
        rgba[..., 3] *= layer['opacity']
        if bld is not None and layer['path'] == coverage:
            rgba[..., 3] *= COVERAGE_FADE
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
        if bld is not None and layer['path'] == coverage:
            # Buildings carry the colour: best frequency per small building, big ones grey
            br = building_renderer(layer['renderer'])
            for subset, alpha in [(bld[~bld['big'] & ~bld['residents_only']], 1.0),
                                  (bld[~bld['big'] & bld['residents_only']], RESIDENTS_ALPHA + 0.15)]:
                if len(subset):
                    b_rgba = render_layer_rgba(subset, br, extent, map_px, dpi)
                    b_rgba[..., 3] *= alpha
                    blend(canvas, b_rgba, 'normal')
                    del b_rgba
            big = bld[bld['big']]
            if len(big):
                b_rgba = render_layer_rgba(big, big_building_renderer(), extent, map_px, dpi)
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
                    residents=bool(args.residents) and args.residents_legend)

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=dpi, facecolor='white')
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == '__main__':
    main()
