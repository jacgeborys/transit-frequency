"""
Render the transit frequency map to PNG using matplotlib.
Creates a dark-themed variant with BMY colorscale (dark blue > magenta > yellow).

Usage:
    python script/render_map.py --city warsaw [--dpi 300]
    python script/render_map.py --city poznan
"""
import argparse
import numpy as np
import geopandas as gpd
import matplotlib.pyplot as plt
from pathlib import Path
from pyproj import Transformer

from cities import get_city, add_city_argument

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent

# BMY colormap (dark blue to bright yellow, for dark theme)
BMY_COLORS = [
    (0,12,124), (0,18,149), (0,21,166), (66,18,166),
    (116,10,152), (149,3,143), (178,0,136), (205,0,129),
    (230,8,121), (249,40,112), (255,71,100), (255,103,87),
    (255,132,71), (255,158,51), (255,181,35), (255,201,28),
    (255,221,29), (255,241,35),
]
BMY_BREAKS = [1, 10, 25, 50, 80, 120, 170, 250, 350, 450, 600, 800,
              1000, 1300, 1700, 2200, 2800, 3500, 100000]


def compute_extent(city: dict):
    """Compute map extent in metric CRS from city bbox."""
    if city.get('render_extent'):
        return city['render_extent']

    # Auto-compute from bbox
    crs = city['crs_metric']
    t = Transformer.from_crs("EPSG:4326", crs, always_xy=True)
    bbox = city['bbox']
    x1, y1 = t.transform(bbox['west'], bbox['south'])
    x2, y2 = t.transform(bbox['east'], bbox['north'])
    # Add 500m padding
    return (x1 - 500, x2 + 500, y1 - 500, y2 + 500)


def load_clipped(path, extent, layer=None, query=None):
    """Load a GeoDataFrame clipped to map extent."""
    kwargs = {}
    if layer:
        kwargs['layer'] = layer
    gdf = gpd.read_file(path, bbox=(extent[0]-1000, extent[2]-1000,
                                     extent[1]+1000, extent[3]+1000), **kwargs)
    if query:
        gdf = gdf.query(query)
    gdf.crs = None
    return gdf


def load_coverage(city: dict, extent_4326):
    """Load coverage map (EPSG:4326) and reproject to metric CRS."""
    # Find most recent data dir
    base = city['data_dir']
    data_dirs = sorted(
        [d for d in base.iterdir() if d.is_dir() and (d / 'coverage_map.gpkg').exists()],
        key=lambda x: x.name, reverse=True
    ) if base.exists() else []
    if not data_dirs:
        print("    WARNING: No coverage_map.gpkg found")
        return gpd.GeoDataFrame()

    path = data_dirs[0] / "coverage_map.gpkg"
    gdf = gpd.read_file(path, bbox=extent_4326)
    if len(gdf) == 0:
        print(f"    WARNING: 0 polygons loaded from {path}")
        return gdf
    gdf = gdf.to_crs(city['crs_metric'])
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


def render_map(city: dict, dpi=300):
    """Render dark-themed map."""
    crs_metric = city['crs_metric']
    osm_dir = city['osm_dir']
    extent = compute_extent(city)

    # Compute 4326 extent for coverage map loading
    t = Transformer.from_crs(crs_metric, "EPSG:4326", always_xy=True)
    lon1, lat1 = t.transform(extent[0] - 1000, extent[2] - 1000)
    lon2, lat2 = t.transform(extent[1] + 1000, extent[3] + 1000)
    extent_4326 = (lon1, lat1, lon2, lat2)

    print(f"Rendering {city['name']} dark theme at {dpi} DPI...")

    bg_color = '#111111'
    building_color = '#222222'
    water_color = '#0a1520'
    water_edge = '#0a1a2a'
    green_color = '#0f1e0f'
    road_color = '#1a1a1a'
    road_major = '#252525'
    rail_color = '#201d1a'

    width_km = (extent[1] - extent[0]) / 1000
    height_km = (extent[3] - extent[2]) / 1000
    aspect = height_km / width_km
    fig_width = 20
    fig, ax = plt.subplots(1, 1, figsize=(fig_width, fig_width * aspect))
    fig.patch.set_facecolor(bg_color)
    ax.set_facecolor(bg_color)
    ax.set_xlim(extent[0], extent[1])
    ax.set_ylim(extent[2], extent[3])
    ax.set_aspect('equal')

    # 1. Green areas
    print("  Loading green areas...")
    for name, alpha in [('allotments',0.5), ('grass',0.5), ('meadow',0.5),
                         ('leisure_relations',0.5), ('cemeteries',0.5),
                         ('parks',0.7), ('forests',0.7)]:
        try:
            gdf = load_clipped(osm_dir / f"{name}.gpkg", extent, layer=name)
            if len(gdf) > 0:
                gdf.plot(ax=ax, color=green_color, alpha=alpha, edgecolor='none', linewidth=0)
        except Exception as e:
            print(f"    Warning: {name}: {e}")

    # 2. Water
    print("  Loading water...")
    try:
        water = load_clipped(osm_dir / "water.gpkg", extent, layer="water")
        if len(water) > 0:
            water.plot(ax=ax, color=water_color, edgecolor=water_edge, linewidth=0.3)
    except Exception as e:
        print(f"    Warning: water: {e}")

    # 3. Coverage map
    print("  Loading coverage map...")
    coverage = load_coverage(city, extent_4326)
    if len(coverage) > 0:
        coverage = coverage[coverage['deduped_trips'] > 0]
        colors = get_coverage_colors(coverage, BMY_BREAKS, BMY_COLORS)
        coverage.plot(ax=ax, color=colors, edgecolor='none', linewidth=0)
        print(f"    {len(coverage)} polygons rendered")

    # 4. Buildings
    print("  Loading buildings...")
    try:
        buildings = load_clipped(osm_dir / "buildings.gpkg", extent, layer="buildings")
        if 'building' in buildings.columns:
            buildings = buildings[~buildings['building'].isin(['roof', 'service'])]
        if len(buildings) > 0:
            buildings.plot(ax=ax, color=building_color, edgecolor='none', linewidth=0)
    except Exception as e:
        print(f"    Warning: buildings: {e}")

    # 5. Roads
    print("  Loading roads...")
    try:
        roads_path = osm_dir / "roads.shp"
        if not roads_path.exists():
            roads_path = osm_dir / "roads.gpkg"
        roads = gpd.read_file(roads_path,
                              bbox=(extent[0]-1000, extent[2]-1000, extent[1]+1000, extent[3]+1000))
        roads.crs = None
        if len(roads) > 0:
            highway_col = next((c for c in ['highway', 'KLASADROG', 'fclass'] if c in roads.columns), None)
            if highway_col:
                major_vals = ['ekspresowa', 'glownaRuchuPrzyspieszonego', 'glowna', 'zbiorcza',
                              'motorway', 'trunk', 'primary', 'secondary']
                major = roads[roads[highway_col].isin(major_vals)]
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
        railways = load_clipped(osm_dir / "railways.gpkg", extent, layer="railways")
        if 'railway' in railways.columns:
            railways = railways[railways['railway'] == 'rail']
        if len(railways) > 0:
            railways.plot(ax=ax, color=rail_color, linewidth=0.5, alpha=0.7)
    except Exception as e:
        print(f"    Warning: railways: {e}")

    ax.axis('off')
    plt.subplots_adjust(left=0, right=1, top=1, bottom=0)

    out_dir = PROJECT_DIR / "png"
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / f"transit_frequency_{city['key']}_dark.png"
    fig.savefig(out_path, dpi=dpi, bbox_inches='tight', pad_inches=0,
                facecolor=fig.get_facecolor())
    plt.close(fig)
    print(f"  Saved: {out_path}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    add_city_argument(parser)
    parser.add_argument('--dpi', type=int, default=300)
    args = parser.parse_args()
    city = get_city(args.city)
    render_map(city, dpi=args.dpi)
    print("Done!")
