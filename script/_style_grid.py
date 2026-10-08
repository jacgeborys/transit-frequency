"""
Style comparison sheet: the same map crop rendered in several style variants.

Usage:
    python _style_grid.py --city warsaw --crop 20.995,52.225,3200,2400 --name warsaw_centre
    -> png/previews/style_<name>/<variant>.png + png/previews/style_grid_<name>.png

Every variant starts from BASE (the current product style) and overrides a few options.
"""
import argparse
import subprocess
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

from cities import get_city, PROJECT_DIR

BASE = {
    '--coverage-fade': '0.45', '--building-shade': '0.80', '--building-saturation': '1.5',
    '--uncovered-rgb': '170,170,170', '--green-rgb': '236,239,236', '--forest-rgb': '232,236,232',
    '--bg-rgb': '241,241,241', '--palette': 'chroma', '--road-edge-mm': '0.1',
}
VARIANTS = [
    ('01 current (chroma)', {}),
    ('02 buildings only, no ground fill', {'--coverage-fade': '0'}),
    ('03 light ground 0.25', {'--coverage-fade': '0.25'}),
    ('04 strong ground 0.75', {'--coverage-fade': '0.75'}),
    ('05 pure class colours (shade 1, sat 1)', {'--building-shade': '1.0', '--building-saturation': '1.0'}),
    ('06 darker buildings (shade 0.6)', {'--building-shade': '0.6'}),
    ('07 turbo', {'--palette': 'turbo', '--building-saturation': '1.0'}),
    ('08 plasma', {'--palette': 'plasma', '--building-saturation': '1.0'}),
    ('09 spectral', {'--palette': 'spectral', '--building-saturation': '1.0'}),
    ('10 rainforest', {'--palette': 'rainforest', '--building-saturation': '1.0'}),
    ('11 tropical', {'--palette': 'tropical', '--building-saturation': '1.0'}),
    ('12 magma', {'--palette': 'magma', '--building-saturation': '1.0'}),
]

# Dark mode: near-black land, dark greens/water, roads as dark cut-outs, glowing palettes
DARK_BASE = {
    '--bg-rgb': '22,23,28', '--green-rgb': '28,36,32', '--forest-rgb': '26,40,33',
    '--uncovered-rgb': '58,58,64', '--coverage-fade': '0.5', '--building-shade': '1.0',
    '--building-saturation': '1.1', '--road-edge-mm': '0.1', '--road-edge-rgb': '70,72,82',
    '--recolor': ['water=24,38,58', 'roads=34,35,42', 'railways=85,85,95', 'buildings=45,46,52'],
}
DARK_VARIANTS = [(f'{i:02d} {name}', {'--palette': name}) for i, name in enumerate([
    'magma_dark', 'inferno_dark', 'plasma_dark', 'chroma_dark', 'ember_dark', 'neon_dark',
    'flamingo_dark', 'bmy_dark', 'synthwave', 'aurora', 'cyberpunk', 'lava'], 1)]
SETS = {'light': (BASE, VARIANTS), 'dark': (DARK_BASE, DARK_VARIANTS)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--city', default='warsaw')
    ap.add_argument('--crop', required=True, help='lon,lat,width_m,height_m')
    ap.add_argument('--name', required=True)
    ap.add_argument('--dpi', default='300')
    ap.add_argument('--cols', type=int, default=4)
    ap.add_argument('--set', default='light', choices=sorted(SETS))
    ap.add_argument('--only', help='Comma-separated variant numbers to (re)render, e.g. 2,5')
    args = ap.parse_args()

    city = get_city(args.city)
    data = sorted(d for d in city['data_dir'].iterdir() if (d / 'coverage_map_gates.gpkg').exists())[-1]
    base, variants = SETS[args.set]
    out_dir = PROJECT_DIR / 'png' / 'previews' / f'style_{args.name}'
    out_dir.mkdir(parents=True, exist_ok=True)
    only = {int(x) for x in args.only.split(',')} if args.only else None

    files = []
    for label, over in variants:
        num = int(label.split()[0])
        f = out_dir / f'{label.split()[0]}.png'
        files.append((label, f))
        if only and num not in only:
            continue
        opts = {**base, **over}
        cmd = [sys.executable, 'render_qgis_style.py', '--city', args.city,
               '--coverage', str(data / 'coverage_map_gates.gpkg'),
               '--buildings', str(data / 'buildings_gates.gpkg'),
               '--restricted-buildings', '--date', '07.10.2026', '--dpi', args.dpi,
               '--crop', args.crop, '--out', str(f)]
        for k, v in opts.items():
            for item in (v if isinstance(v, list) else [v]):
                cmd += [k, item]
        print(f'{label}...', flush=True)
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL,
                       cwd=Path(__file__).resolve().parent)

    tiles = [(label, Image.open(f).convert('RGB')) for label, f in files if f.exists()]
    w, h = tiles[0][1].size
    pad, head = 16, 44
    rows = -(-len(tiles) // args.cols)
    sheet = Image.new('RGB', (args.cols * (w + pad) + pad, rows * (h + head + pad) + pad), 'white')
    draw = ImageDraw.Draw(sheet)
    try:
        font = ImageFont.truetype('arial.ttf', 26)
    except OSError:
        font = ImageFont.load_default()
    for i, (label, im) in enumerate(tiles):
        x = pad + (i % args.cols) * (w + pad)
        y = pad + (i // args.cols) * (h + head + pad)
        draw.text((x, y + 6), label, fill='black', font=font)
        sheet.paste(im, (x, y + head))
    out = PROJECT_DIR / 'png' / 'previews' / f'style_grid_{args.name}.png'
    sheet.save(out)
    print(f'Saved {out}')


if __name__ == '__main__':
    main()
