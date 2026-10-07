"""Combine png/previews/palettes/krakow_centre_<palette>.png into one labelled grid image."""
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

D = Path(__file__).resolve().parent.parent / 'png' / 'previews' / 'palettes'
files = sorted(D.glob('krakow_centre_*.png'))
files.sort(key=lambda f: (f.stem != 'krakow_centre_qgis', f.stem))  # current palette first
if not files:
    raise SystemExit('no previews found')
thumbs = [Image.open(f).convert('RGB') for f in files]
w = 900
thumbs = [t.resize((w, int(t.height * w / t.width)), Image.LANCZOS) for t in thumbs]
h = thumbs[0].height
cols = 3
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
    draw.text((x + 4, y + 6), f.stem.replace('krakow_centre_', ''), fill='black', font=font)
    grid.paste(t, (x, y + head))
out = D / 'palette_grid.png'
grid.save(out)
print(f'Saved {out} ({len(files)} palettes)')
