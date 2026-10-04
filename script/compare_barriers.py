"""
Compare the plain-buffer coverage map with the barrier-aware one.

Prints how much covered area / population-relevant area changes and renders
side-by-side close-ups (buffer | barriers) with fences and buildings drawn.

Usage:
    python compare_barriers.py --city warsaw [data_folder]
Outputs png/compare_barriers_<city>_<area>.png
"""
import argparse
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.colors import BoundaryNorm, ListedColormap
from pyproj import Transformer

from cities import get_city, add_city_argument, PROJECT_DIR

BREAKS = [1, 10, 25, 50, 80, 120, 170, 250, 350, 450, 600, 800,
          1000, 1300, 1700, 2200, 2800, 3500, 100000]
# Light-theme ramp close to the published QGIS style (yellow > pink > purple > navy)
COLORS = ['#fff3c4', '#fde9b0', '#fbdc9f', '#f9cd93', '#f7bd8c', '#f6ab8a', '#f4988c',
          '#f08591', '#ea7298', '#e15f9f', '#d44ea6', '#c341ab', '#ae37af', '#9531b1',
          '#7a30b0', '#5a33ab', '#3a35a0', '#222f8a']

# Close-up areas: name -> (lon, lat) centre; 1.6 km x 1.6 km windows
AREAS = {
    'warsaw': {
        'wilanow': (21.103, 52.155),
        'bialoleka_tarchomin': (20.955, 52.318),
        'mokotow_sluzewiec': (21.003, 52.180),
        'srodmiescie': (21.010, 52.230),
    },
    'krakow': {
        'stare_miasto': (19.937, 50.061),
        'ruczaj': (19.900, 50.020),
        'nowa_huta': (20.035, 50.074),
    },
}
HALF = 800


def main():
    parser = argparse.ArgumentParser(description='Compare buffer vs barrier coverage')
    add_city_argument(parser)
    parser.add_argument('data_folder', nargs='?')
    args = parser.parse_args()
    city = get_city(args.city)
    crs = city['crs_metric']

    data_dir = Path(args.data_folder) if args.data_folder else sorted(
        d for d in city['data_dir'].iterdir()
        if (d / 'coverage_map_barriers.gpkg').exists())[-1]
    base = gpd.read_file(data_dir / 'coverage_map.gpkg').to_crs(crs)
    barr = gpd.read_file(data_dir / 'coverage_map_barriers.gpkg').to_crs(crs)

    # --- Area statistics per frequency class --------------------------------
    print(f"{city['name']} — {data_dir.name}")
    print(f"{'departures':>12} {'buffer ha':>10} {'barrier ha':>11} {'change':>8}")
    tot_b = tot_r = 0
    for lo, hi in zip([1, 100, 500, 1000, 2000], [100, 500, 1000, 2000, 10**6]):
        a = base[(base.deduped_trips >= lo) & (base.deduped_trips < hi)].area.sum() / 1e4
        b = barr[(barr.deduped_trips >= lo) & (barr.deduped_trips < hi)].area.sum() / 1e4
        tot_b += a
        tot_r += b
        print(f"{lo:>5}-{hi if hi < 10**6 else '':<6} {a:>10.0f} {b:>11.0f} {(b / a - 1) * 100 if a else 0:>7.1f}%")
    print(f"{'total':>12} {tot_b:>10.0f} {tot_r:>11.0f} {(tot_r / tot_b - 1) * 100:>7.1f}%")

    # --- Close-ups ----------------------------------------------------------
    to_m = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
    cmap = ListedColormap(COLORS)
    norm = BoundaryNorm(BREAKS, cmap.N)
    out_dir = PROJECT_DIR / 'png'
    for name, (lon, lat) in AREAS.get(city['key'], {}).items():
        cx, cy = to_m.transform(lon, lat)
        bbox = (cx - HALF, cy - HALF, cx + HALF, cy + HALF)
        bbox4326 = Transformer.from_crs(crs, 'EPSG:4326', always_xy=True).transform_bounds(*bbox)
        bld = gpd.read_file(city['osm_dir'] / 'buildings.gpkg', bbox=bbox4326).to_crs(crs)
        fen = gpd.read_file(city['osm_dir'] / 'barriers.gpkg', bbox=bbox4326).to_crs(crs)
        fen['geometry'] = fen.geometry.boundary.where(
            fen.geom_type.isin(['Polygon', 'MultiPolygon']), fen.geometry)

        fig, axes = plt.subplots(1, 2, figsize=(16, 8.4), dpi=150)
        for ax, gdf, title in [(axes[0], base, 'Bufor 50 m (obecnie)'),
                               (axes[1], barr, 'Płoty = bariery, budynki: 10 m')]:
            sub = gdf.cx[bbox[0]:bbox[2], bbox[1]:bbox[3]]
            sub.plot(ax=ax, column='deduped_trips', cmap=cmap, norm=norm, linewidth=0)
            bld.plot(ax=ax, facecolor='none', edgecolor='#333333', linewidth=0.3)
            fen.plot(ax=ax, color='#0a7a2f', linewidth=0.8)
            ax.set_xlim(bbox[0], bbox[2])
            ax.set_ylim(bbox[1], bbox[3])
            ax.set_title(title, fontsize=13)
            ax.set_axis_off()
        fig.suptitle(f"{city['name']} — {name.replace('_', ' ')}  (zielone linie = płoty/mury OSM)",
                     fontsize=14)
        fig.tight_layout()
        out = out_dir / f"compare_barriers_{city['key']}_{name}.png"
        fig.savefig(out, facecolor='white')
        plt.close(fig)
        print(f"Saved {out}")


if __name__ == '__main__':
    main()
