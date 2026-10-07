"""Combine <dir>/<prefix><palette>.png previews into one labelled grid image.

Usage: python _palette_grid.py [dir] [prefix]
       (default: png/previews/palettes krakow_centre_)
"""
import sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parent.parent
D = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / 'png' / 'previews' / 'palettes'
PREFIX = sys.argv[2] if len(sys.argv) > 2 else 'krakow_centre_'
files = sorted(D.glob(f'{PREFIX}*.png'))
files.sort(key=lambda f: (f.stem != f'{PREFIX}qgis', f.stem))  # current palette first
if not files:
    raise SystemExit('no previews found')
thumbs = [Image.open(f).convert('RGB') for f in files]
w = 900
thumbs = [t.resize((w, int(t.height * w / t.width)), Image.LANCZOS) for t in thumbs]
h = thumbs[0].height
cols = 3 if len(thumbs) <= 9 else 4
rows = (len(thumbs) + cols - 1) // cols
pad, head = 12, 44
grid = Image.new('RGB', (cols * (w + pad) + pad, rows * (h + head + pad) + pad), 'white')
draw = ImageDraw.Draw(grid)
try:
    font = ImageFont.truetype('arial.ttf', 30)
except OSError:
    font = None
for i, (f, t) in enumerate(zip(files, thumbs)):
    x = pad + (i % cols) * (w + pad)
    y = pad + (i // cols) * (h + head + pad)
    draw.text((x + 4, y + 6), f.stem.replace(PREFIX, ''), fill='black', font=font)
    grid.paste(t, (x, y + head))
out = D / 'palette_grid.png'
grid.save(out)
print(f'Saved {out} ({len(files)} palettes)')
