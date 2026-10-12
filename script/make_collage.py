"""
Collage of several 300 dpi finals in justified rows (each row scaled to the full width, so the
maps in a row share one height), on the posters' black page.

Writes png/<name>.png and a JPEG under 20 MB in png/compressed/<name>.jpg.

Usage:
    python make_collage.py                       # the default 8 cities, 2 rows
    python make_collage.py --rows "warsaw,krakow;paris,zurich" --name collage_test --width 4000
"""
import argparse

from PIL import Image

from cities import PROJECT_DIR
from compress_finals import compress

Image.MAX_IMAGE_PIXELS = None

DEFAULT_ROWS = 'warsaw,krakow,berlin,amsterdam;paris,zurich,newyork,sanfrancisco'


def final_png(city):
    png_dir = PROJECT_DIR / 'png'
    pattern = 'how_many_rides_in_5_mins_2026_*_hq.png' if city == 'warsaw' else \
        f'how_many_rides_in_5_mins_{city}_*_hq.png'
    files = sorted(png_dir.glob(pattern))
    if not files:
        raise FileNotFoundError(f'No final for {city} in {png_dir}')
    return files[-1]


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--rows', default=DEFAULT_ROWS, help='Cities, rows separated by ";"')
    ap.add_argument('--width', type=int, default=8000, help='Collage width in px')
    ap.add_argument('--gap', type=int, default=40, help='Gap between maps and around the edge, px')
    ap.add_argument('--name', default='collage_8_cities')
    args = ap.parse_args()

    rows = [[c.strip() for c in r.split(',')] for r in args.rows.split(';')]
    files = {c: final_png(c) for r in rows for c in r}
    sizes = {c: Image.open(f).size for c, f in files.items()}

    # Row height so that the row's maps + gaps fill the width exactly
    layout = []
    for r in rows:
        inner = args.width - args.gap * (len(r) + 1)
        h = inner / sum(sizes[c][0] / sizes[c][1] for c in r)
        layout.append((r, h))
    total_h = round(sum(h for _, h in layout) + args.gap * (len(rows) + 1))

    out = Image.new('RGB', (args.width, total_h), (0, 0, 0))
    y = args.gap
    for r, h in layout:
        x = args.gap
        for i, c in enumerate(r):
            w = round(sizes[c][0] / sizes[c][1] * h) if i < len(r) - 1 else args.width - args.gap - x
            im = Image.open(files[c]).convert('RGB').resize((w, round(h)), Image.LANCZOS)
            out.paste(im, (x, round(y)))
            print(f'{c}: {w}x{round(h)} at {x},{round(y)}', flush=True)
            x += w + args.gap
        y += h + args.gap

    png = PROJECT_DIR / 'png' / f'{args.name}.png'
    out.save(png, optimize=True)
    jpg = PROJECT_DIR / 'png' / 'compressed' / f'{args.name}.jpg'
    jpg.parent.mkdir(exist_ok=True)
    data, q, scale, size = compress(png, jpg, 20 * 2**20 - 64 * 1024)
    jpg.write_bytes(data)
    print(f'Saved {png.name} ({out.width}x{out.height}, {png.stat().st_size / 2**20:.1f} MB) and '
          f'{jpg.name} ({len(data) / 2**20:.1f} MB, q{q}{"" if scale == 1 else f", scaled {scale:.2f}"})')


if __name__ == '__main__':
    main()
