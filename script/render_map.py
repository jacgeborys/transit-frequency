"""
Render the transit frequency map to PNG using matplotlib.
Creates a dark-themed variant with BMY colorscale (dark blue > magenta > yellow).

Replicates layer styling from transit-frequency-map.qgz.

Usage:
    python script/render_map.py [--dpi 300]
"""
import sys
import argparse
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
from pyproj import Transformer

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
OSM_DIR = Path("D:/QGIS/mapy_warszawy_misc/data/osm")

# Layout "Lines_Frequency_Waw" extent (EPSG:2180)
EXTENT = (623233.8, 651733.8, 477610.7, 500410.7)  # xmin, xmax, ymin, ymax

# Convert extent to EPSG:4326 for coverage_map (which is in WGS84)
_t = Transformer.from_crs("EPSG:2180", "EPSG:4326", always_xy=True)
_lon1, _lat1 = _t.transform(EXTENT[0] - 1000, EXTENT[2] - 1000)
_lon2, _lat2 = _t.transform(EXTENT[1] + 1000, EXTENT[3] + 1000)
EXTENT_4326 = (_lon1, _lat1, _lon2, _lat2)

# Transformer for reprojecting 4326 -> 2180
T_TO_2180 = Transformer.from_crs("EPSG:4326", "EPSG:2180", always_xy=True)

# BMY colormap (NOT reversed — dark blue to bright yellow, for dark theme)
BMY_COLORS = [
    (0,12,124), (0,18,149), (0,21,166), (66,18,166),
    (116,10,152), (149,3,143), (178,0,136), (205,0,129),
    (230,8,121), (249,40,112), (255,71,100), (255,103,87),
    (255,132,71), (255,158,51), (255,181,35), (255,201,28),
    (255,221,29), (255,241,35),
]
BMY_BREAKS = [1, 10, 25, 50, 80, 120, 170, 250, 350, 450, 600, 800,
              1000, 1300, 1700, 2200, 2800, 3500, 100000]


def load_clipped(path, layer=None, query=None):
    """Load a GeoDataFrame clipped to map extent (EPSG:2180)."""
    kwargs = {}
    if layer:
        kwargs['layer'] = layer
    gdf = gpd.read_file(path, bbox=(EXTENT[0]-1000, EXTENT[2]-1000,
                                     EXTENT[1]+1000, EXTENT[3]+1000), **kwargs)
    if query:
        gdf = gdf.query(query)
    gdf.crs = None
    return gdf


def load_coverage():
    """Load coverage map (EPSG:4326) and reproject to 2180."""
    path = PROJECT_DIR / "_data/2026_08_02/coverage_map.gpkg"
    gdf = gpd.read_file(path, bbox=EXTENT_4326)
    if len(gdf) == 0:
        print(f"    WARNING: 0 polygons loaded from {path}")
        return gdf
    # Reproject to 2180
    gdf = gdf.to_crs("EPSG:2180")
    gdf.crs = None
    return gdf


def get_coverage_colors(gdf, breaks, colors):
    """Assign BMY colors based on deduped_trips."""
    rgba_list = []
    for val in gdf['deduped_trips']:
        color_idx = 0
        for i in range(len(breaks) - 1):
            if val >= breaks[i]:
                color_idx = i
        r, g, b = colors[color_idx]
        rgba_list.append((r/255, g/255, b/255, 0.9))
    return rgba_list


def render_map(dpi=300):
    """Render dark-themed map."""
    print(f"Rendering dark theme at {dpi} DPI...")

    # Dark theme colors
    bg_color = '#111111'
    building_color = '#222222'
    water_color = '#0a1520'
    water_edge = '#0a1a2a'
    green_color = '#0f1e0f'
    road_color = '#1a1a1a'
    road_major = '#252525'
    rail_color = '#201d1a'

    # Figure setup
    width_km = (EXTENT[1] - EXTENT[0]) / 1000
    height_km = (EXTENT[3] - EXTENT[2]) / 1000
    aspect = height_km / width_km
    fig_width = 20
    fig, ax = plt.subplots(1, 1, figsize=(fig_width, fig_width * aspect))
    fig.patch.set_facecolor(bg_color)
    ax.set_facecolor(bg_color)
    ax.set_xlim(EXTENT[0], EXTENT[1])
    ax.set_ylim(EXTENT[2], EXTENT[3])
    ax.set_aspect('equal')

    # -- Render layers bottom to top --

    # 1. Green areas
    print("  Loading green areas...")
    for name, alpha in [('allotments',0.5), ('grass',0.5), ('meadow',0.5),
                         ('leisure_relations',0.5), ('cemeteries',0.5),
                         ('parks',0.7), ('forests',0.7)]:
        try:
            gdf = load_clipped(OSM_DIR / f"{name}.gpkg", layer=name)
            if len(gdf) > 0:
                gdf.plot(ax=ax, color=green_color, alpha=alpha, edgecolor='none', linewidth=0)
        except Exception as e:
            print(f"    Warning: {name}: {e}")

    # 2. Water
    print("  Loading water...")
    try:
        water = load_clipped(OSM_DIR / "water.gpkg", layer="water")
        if len(water) > 0:
            water.plot(ax=ax, color=water_color, edgecolor=water_edge, linewidth=0.3)
    except Exception as e:
        print(f"    Warning: water: {e}")

    # 3. Coverage map
    print("  Loading coverage map...")
    coverage = load_coverage()
    if len(coverage) > 0:
        coverage = coverage[coverage['deduped_trips'] > 0]
        colors = get_coverage_colors(coverage, BMY_BREAKS, BMY_COLORS)
        coverage.plot(ax=ax, color=colors, edgecolor='none', linewidth=0)
        print(f"    {len(coverage)} polygons rendered")

    # 4. Buildings (on top of coverage for structure)
    print("  Loading buildings...")
    try:
        buildings = load_clipped(OSM_DIR / "buildings.gpkg", layer="buildings")
        buildings = buildings[~buildings['building'].isin(['roof', 'service'])]
        if len(buildings) > 0:
            buildings.plot(ax=ax, color=building_color, edgecolor='none', linewidth=0)
    except Exception as e:
        print(f"    Warning: buildings: {e}")

    # 5. Roads
    print("  Loading roads...")
    try:
        roads = gpd.read_file(OSM_DIR / "roads.shp",
                              bbox=(EXTENT[0]-1000, EXTENT[2]-1000, EXTENT[1]+1000, EXTENT[3]+1000))
        roads.crs = None
        if len(roads) > 0:
            col = 'KLASADROG' if 'KLASADROG' in roads.columns else 'fclass'
            if col in roads.columns:
                major_vals = ['ekspresowa', 'glownaRuchuPrzyspieszonego', 'glowna', 'zbiorcza',
                              'motorway', 'trunk', 'primary', 'secondary']
                major = roads[roads[col].isin(major_vals)]
                minor = roads[~roads.index.isin(major.index)]
            else:
                major = gpd.GeoDataFrame()
                minor = roads
            if len(minor) > 0:
                minor.plot(ax=ax, color=road_color, linewidth=0.15, alpha=0.8)
            if len(major) > 0:
                major.plot(ax=ax, color=road_major, linewidth=0.5, alpha=0.9)
    except Exception as e:
        print(f"    Warning: roads: {e}")

    # 6. Railways
    print("  Loading railways...")
    try:
        railways = load_clipped(OSM_DIR / "railways.gpkg", layer="railways")
        if 'railway' in railways.columns:
            railways = railways[railways['railway'] == 'rail']
        if len(railways) > 0:
            railways.plot(ax=ax, color=rail_color, linewidth=0.5, alpha=0.7)
    except Exception as e:
        print(f"    Warning: railways: {e}")

    # Finalize
    ax.axis('off')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    out_dir = PROJECT_DIR / "png"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "transit_frequency_dark.png"
    fig.savefig(out_path, dpi=dpi, bbox_inches='tight', pad_inches=0,
                facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved: {out_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--dpi', type=int, default=300)
    args = parser.parse_args()
    render_map(dpi=args.dpi)
    print("Done!")
