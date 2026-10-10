"""
Compare two coverage maps (e.g. vector 04 vs raster 04) pixel by pixel.

Both are burnt into one grid (default 2 m) by deduped_trips; prints how many covered pixels
agree exactly, how many land in the same colour class (breaks of styles/transit_frequency.qml),
how many are covered by only one map, and writes a difference map PNG.

Usage:
    python compare_coverage.py --city lublin A.gpkg B.gpkg [--png png/previews/x.png]
"""
import argparse
import importlib
import re
from pathlib import Path

import geopandas as gpd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import rasterio.features
from rasterio.transform import from_origin

from cities import get_city, add_city_argument

STYLE = Path(__file__).resolve().parent.parent / 'styles' / 'transit_frequency.qml'


def class_breaks():
    lows = sorted({float(v) for v in re.findall(r'lower="([0-9.]+)"', STYLE.read_text(encoding='utf-8'))})
    return np.array(lows[1:])   # upper edges; class = searchsorted


def burn(gdf, transform, shape):
    shapes = sorted(zip(gdf.geometry, gdf['deduped_trips']), key=lambda t: t[1])
    return rasterio.features.rasterize(shapes, out_shape=shape, transform=transform, fill=0, dtype='int32')


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    ap.add_argument('a')
    ap.add_argument('b')
    ap.add_argument('--res', type=float, default=2.0)
    ap.add_argument('--png')
    args = ap.parse_args()
    crs = get_city(args.city)['crs_metric']
    a, b = (gpd.read_file(f).to_crs(crs) for f in (args.a, args.b))
    minx, miny = np.minimum(a.total_bounds[:2], b.total_bounds[:2])
    maxx, maxy = np.maximum(a.total_bounds[2:], b.total_bounds[2:])
    # Same alignment as 04_coverage_raster.py (cell edges on the isochrones' vertex lattice),
    # so comparison pixels coincide with the raster method's cells and never sit on edges
    lattice = importlib.import_module('04_coverage_raster')._lattice_origin
    xy = a.geometry.get_coordinates()
    minx = lattice((minx, xy['x'].to_numpy()[:200000]), args.res)
    miny = lattice((miny, xy['y'].to_numpy()[:200000]), args.res)
    maxy = miny + np.ceil((maxy - miny) / args.res) * args.res
    w, h = int((maxx - minx) / args.res) + 1, int((maxy - miny) / args.res) + 1
    t = from_origin(minx, maxy, args.res, args.res)
    A, B = burn(a, t, (h, w)), burn(b, t, (h, w))
    cov_a, cov_b = A > 0, B > 0
    both = cov_a & cov_b
    px_ha = args.res ** 2 / 10000
    br = class_breaks()
    ca, cb = np.searchsorted(br, A, side='right'), np.searchsorted(br, B, side='right')
    n = both.sum()
    print(f"covered: A {cov_a.sum() * px_ha:,.0f} ha, B {cov_b.sum() * px_ha:,.0f} ha, "
          f"only A {(cov_a & ~cov_b).sum() * px_ha:,.1f} ha, only B {(cov_b & ~cov_a).sum() * px_ha:,.1f} ha")
    print(f"pixels covered by both: exact value {np.mean(A[both] == B[both]) * 100:.2f} %, "
          f"same colour class {np.mean(ca[both] == cb[both]) * 100:.2f} %, "
          f"class off by 1: {np.mean(np.abs(ca[both] - cb[both]) == 1) * 100:.2f} %, "
          f"by 2+: {np.mean(np.abs(ca[both] - cb[both]) >= 2) * 100:.3f} %")
    rel = np.abs(A[both] - B[both]) / np.maximum(A[both], B[both])
    print(f"relative difference where both cover: median {np.median(rel) * 100:.2f} %, "
          f"99th pct {np.percentile(rel, 99) * 100:.1f} %, max {rel.max() * 100:.0f} %  ({n:,} px)")
    if args.png:
        diff = np.zeros((h, w, 3), dtype=np.uint8)
        diff[both] = (60, 60, 60)                                   # agree (same class): grey
        diff[both & (ca != cb)] = (255, 200, 0)                     # different class: yellow
        diff[cov_a & ~cov_b] = (255, 60, 60)                        # only A: red
        diff[cov_b & ~cov_a] = (60, 160, 255)                       # only B: blue
        step = max(1, max(h, w) // 4000)
        plt.figure(figsize=(12, 12 * h / w))
        plt.imshow(diff[::step, ::step], interpolation='nearest')
        plt.axis('off')
        plt.title(f"{Path(args.a).name} vs {Path(args.b).name}: grey = same class, "
                  f"yellow = other class, red = only A, blue = only B", fontsize=9)
        plt.savefig(args.png, dpi=150, bbox_inches='tight', facecolor='white')
        print(f"difference map -> {args.png}")


if __name__ == '__main__':
    main()
