"""
2x2 panel of access sketches (sketch_access.py ... --no-legend) with one shared legend below.

Writes png/<name>.png and a JPEG under 20 MB in png/compressed/<name>.jpg.

Usage:
    python make_sketch_panel.py                     # the four default tiles in png/previews/
    python make_sketch_panel.py --tiles a.png b.png c.png d.png --name sketch_panel_test
"""
import argparse

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
from PIL import Image

from cities import PROJECT_DIR
from compress_finals import compress

PREV = PROJECT_DIR / 'png' / 'previews'
DEFAULT_TILES = [PREV / f for f in (
    'sketch_warsaw_pl_wilsona_tile.png', 'sketch_krakow_cichy_kacik_tile.png',
    'sketch_sanfrancisco_hyde_st_lombard_st_tile.png', 'sketch_newyork_times_sq_42_st_tile.png')]

# Same colours as sketch_access.py --paths none --building-status
HANDLES = [
    Patch(color='#c6b6ea', label='5-min walk, public'),
    Patch(color='#f0c8e6', label='5-min walk, residents only'),
    Patch(color='#5b2c8f', label='building reached'),
    Patch(facecolor='white', edgecolor='#a8a8a8', hatch='//////', label='building not reached'),
    Line2D([], [], color='#263238', lw=1.2, label='fence / wall'),
    Line2D([], [], ls='', marker='s', ms=6, mfc='#e53935', mec='#e53935', label='closed gate'),
    Line2D([], [], ls='', marker='o', ms=6, mfc='#263238', mec='#263238', label='open gate'),
    Line2D([], [], ls='', marker='o', ms=11, mfc='#f5b700', mec='#f5b700', label='stop'),
    Line2D([], [], color='#1a1a2e', lw=2.4, ls=(0, (0.6, 0.6)), label='stairs'),
    Line2D([], [], color='#1a1a2e', lw=1.3, ls=(0, (5, 3)), label='5-min walk if it were flat'),
]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--tiles', nargs=4, default=[str(t) for t in DEFAULT_TILES])
    ap.add_argument('--name', default='sketch_panel_4')
    ap.add_argument('--dpi', type=int, default=220)
    args = ap.parse_args()

    tiles = [Image.open(t).convert('RGB') for t in args.tiles]
    tw = max(t.width for t in tiles)
    th = max(t.height for t in tiles)
    legend_h = 0.09 * th
    fig = plt.figure(figsize=(2 * tw / args.dpi, (2 * th + legend_h) / args.dpi), dpi=args.dpi)
    fig.patch.set_facecolor('white')
    bottom = legend_h / (2 * th + legend_h)
    for i, t in enumerate(tiles):
        r, c = divmod(i, 2)
        ax = fig.add_axes([c / 2, bottom + (1 - r) * (1 - bottom) / 2, 0.5, (1 - bottom) / 2])
        ax.imshow(t)
        ax.set_axis_off()
    fig.legend(handles=HANDLES, loc='lower center', ncol=5, frameon=False,
               fontsize=11, bbox_to_anchor=(0.5, 0.005), handlelength=2.2, columnspacing=2.0)

    png = PROJECT_DIR / 'png' / f'{args.name}.png'
    fig.savefig(png, dpi=args.dpi, facecolor='white')
    jpg = PROJECT_DIR / 'png' / 'compressed' / f'{args.name}.jpg'
    jpg.parent.mkdir(exist_ok=True)
    data, q, scale, size = compress(png, jpg, 20 * 2**20 - 64 * 1024)
    jpg.write_bytes(data)
    print(f'Saved {png.name} and {jpg.name} ({size[0]}x{size[1]}, {len(data) / 2**20:.1f} MB, q{q})')


if __name__ == '__main__':
    main()
