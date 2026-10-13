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
# "Wow" set (2026-10-13): the current poster vs pure Schwarzplan variants in the spirit of the
# viral 2023 rail maps (buildings only, flat colours, few classes, no fills/halos/roads).
# None = flag without value; DROP = remove the option from the base.
DROP = object()
GROUND = ['roads', 'railways', 'parks', 'forests', 'grass', 'meadow', 'allotments', 'cemeteries',
          'leisure', 'leisure_relations', 'waterways']
WOW_BASE = {  # = render_poster.STYLE (map part)
    '--bg-rgb': '0,0,0', '--green-rgb': '30,44,34', '--forest-rgb': '28,50,37',
    '--uncovered-rgb': '58,58,64', '--coverage-fade': '0.42', '--min-fill-lightness': '7',
    '--building-shade': '1.0', '--building-saturation': '1.1',
    '--road-edge-mm': '0.06', '--road-edge-rgb': '66,68,80',
    '--recolor': ['water=22,39,63', 'roads=80,82,94', 'railways=100,100,112', 'buildings=45,46,52'],
    '--line-scale': 'railways=0.6',
    '--building-outline-mm': '0.05', '--building-outline-rgb': '95,96,105', '--building-outline-spill': '0.35',
    '--palette': 'bmy_dark', '--min-lightness': '40', '--min-lightness-step': '1.6',
    '--palette-extend': '#fff38a,#fffbd6', '--roads-under-buildings': None,
}
SCHWARZ = {'--coverage-fade': '0', '--hide': GROUND, '--building-outline-mm': '0', '--road-edge-mm': '0'}
SIX = {'--breaks': '25,100,250,600,1500', '--min-lightness': '42', '--min-lightness-step': '8'}
THREE = {'--breaks': '100,500', '--min-lightness': '45', '--min-lightness-step': '15'}
LIGHT = {'--bg-rgb': '255,255,255', '--uncovered-rgb': '212,212,216',
         '--recolor': ['water=255,255,255', 'buildings=212,212,216'],
         '--min-lightness': DROP, '--min-lightness-step': DROP, '--palette-extend': DROP,
         '--min-fill-lightness': DROP}
WOW_VARIANTS = [
    ('01 current poster', {}),
    ('02 no ground fill', {'--coverage-fade': '0'}),
    ('03 dark Schwarzplan, 19 classes', {**SCHWARZ}),
    ('04 dark Schwarzplan, 6 classes', {**SCHWARZ, **SIX}),
    ('05 dark, 6 classes, plasma', {**SCHWARZ, **SIX, '--palette': 'plasma_dark', '--palette-extend': DROP}),
    ('06 dark, 6 classes, neon', {**SCHWARZ, **SIX, '--palette': 'neon_dark', '--palette-extend': DROP}),
    ('07 dark, 3 classes', {**SCHWARZ, **THREE}),
    ('08 dark, 6 cl., darker grey', {**SCHWARZ, **SIX, '--uncovered-rgb': '34,34,38',
                                     '--recolor': ['water=14,24,40', 'buildings=34,34,38']}),
    ('09 light Schwarzplan, 19 cl. plasma', {**SCHWARZ, **LIGHT, '--palette': 'plasma'}),
    ('10 light, 6 classes, plasma', {**SCHWARZ, **LIGHT, '--palette': 'plasma', '--breaks': SIX['--breaks']}),
    ('11 light, 6 classes, turbo', {**SCHWARZ, **LIGHT, '--palette': 'turbo', '--breaks': SIX['--breaks']}),
    ('12 light, 3 classes, plasma', {**SCHWARZ, **LIGHT, '--palette': 'plasma', '--breaks': THREE['--breaks']}),
]
SETS = {'light': (BASE, VARIANTS), 'dark': (DARK_BASE, DARK_VARIANTS), 'wow': (WOW_BASE, WOW_VARIANTS)}

# "Wow 2" (2026-10-13): user liked 08 but wants MORE classes, a land a step above black with
# pitch-black roads, versions without roads, and a slightly adjusted colour scale
FINE = '1,10,25,40,60,80,100,125,150,200,250,300,375,450,550,650,800,1000,1250,1500,1800,2200,2800,3500,4500,6000'
W08 = {**WOW_BASE, **SCHWARZ, '--uncovered-rgb': '34,34,38',
       '--recolor': ['water=14,24,40', 'buildings=34,34,38', 'roads=0,0,0']}
NOROADS = {}                                   # SCHWARZ hides roads already
GREY_LAND = lambda rgb: {'--bg-rgb': rgb, '--hide': [g for g in GROUND if g != 'roads']}
FINE_L = {'--breaks': FINE, '--min-lightness': '40', '--min-lightness-step': '1.2'}
WOW2_VARIANTS = [
    ('01 08 as is (19 classes, no roads)', {}),
    ('02 26 classes, no roads', {**FINE_L}),
    ('03 26 cl., land 16, black roads', {**FINE_L, **GREY_LAND('16,16,19')}),
    ('04 26 cl., land 24, black roads', {**FINE_L, **GREY_LAND('24,24,28')}),
    ('05 26 cl., land 32, black roads', {**FINE_L, **GREY_LAND('32,32,37'), '--uncovered-rgb': '48,48,54',
                                         '--recolor': ['water=14,24,40', 'buildings=48,48,54', 'roads=0,0,0']}),
    ('06 = 03, top stays gold (no cream)', {**FINE_L, **GREY_LAND('16,16,19'), '--palette-extend': '#ffd23f,#ffe680'}),
    ('07 = 03, deeper lows (floor 30)', {**FINE_L, **GREY_LAND('16,16,19'), '--min-lightness': '30', '--min-lightness-step': '1.6'}),
    ('08 = 03, saturation 1.35', {**FINE_L, **GREY_LAND('16,16,19'), '--building-saturation': '1.35'}),
    ('09 = 03, plasma', {**FINE_L, **GREY_LAND('16,16,19'), '--palette': 'plasma_dark', '--palette-extend': '#fcffa4'}),
    ('10 = 03, white-hot top', {**FINE_L, **GREY_LAND('16,16,19'), '--palette-extend': '#fff38a,#ffffff'}),
    ('11 = 04, gold top + sat 1.25', {**FINE_L, **GREY_LAND('24,24,28'), '--palette-extend': '#ffd23f,#ffe680',
                                     '--building-saturation': '1.25'}),
    ('12 = 02, gold top + sat 1.25', {**FINE_L, '--palette-extend': '#ffd23f,#ffe680', '--building-saturation': '1.25'}),
]
SETS['wow2'] = (W08, WOW2_VARIANTS)

# "Wow 3" (2026-10-13): wow2's 08 (26 classes, land 16, black roads, saturation 1.35) with the
# walk-area fill back (the visible trace of the path/fence analysis), at several strengths
W3 = {**W08, **FINE_L, **GREY_LAND('16,16,19'), '--building-saturation': '1.35'}
WOW3_VARIANTS = [
    ('01 no fill (wow2 08)', {}),
    ('02 fill 0.20', {'--coverage-fade': '0.20'}),
    ('03 fill 0.30', {'--coverage-fade': '0.30'}),
    ('04 fill 0.42 (old pale)', {'--coverage-fade': '0.42'}),
]
SETS['wow3'] = (W3, WOW3_VARIANTS)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--city', default='warsaw')
    ap.add_argument('--crop', required=True, help='lon,lat,width_m,height_m')
    ap.add_argument('--name', required=True)
    ap.add_argument('--dpi', default='300')
    ap.add_argument('--cols', type=int, default=4)
    ap.add_argument('--set', default='light', choices=sorted(SETS))  # SETS is filled at import
    ap.add_argument('--root', default=str(PROJECT_DIR / 'png' / 'previews'),
                    help='Folder for style_<name>/ and style_grid_<name>.png (style lab: png/style_lab)')
    ap.add_argument('--only', help='Comma-separated variant numbers to (re)render, e.g. 2,5')
    args = ap.parse_args()

    city = get_city(args.city)
    data = sorted(d for d in city['data_dir'].iterdir() if (d / 'coverage_map_gates.gpkg').exists())[-1]
    base, variants = SETS[args.set]
    out_dir = Path(args.root) / f'style_{args.name}'
    out_dir.mkdir(parents=True, exist_ok=True)
    only = {int(x) for x in args.only.split(',')} if args.only else None

    files = []
    for label, over in variants:
        num = int(label.split()[0])
        f = out_dir / f'{label.split()[0]}.png'
        files.append((label, f))
        if only and num not in only:
            continue
        opts = {k: v for k, v in {**base, **over}.items() if v is not globals().get('DROP')}
        cmd = [sys.executable, 'render_qgis_style.py', '--city', args.city,
               '--coverage', str(data / 'coverage_map_gates.gpkg'),
               '--buildings', str(data / 'buildings_gates.gpkg'),
               '--restricted-buildings', '--date', '07.10.2026', '--dpi', args.dpi,
               f'--crop={args.crop}', '--out', str(f)]  # = form: western longitudes start with '-'
        for k, v in opts.items():
            if v is None:  # flag without a value
                cmd.append(k)
                continue
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
    out = Path(args.root) / f'style_grid_{args.name}.png'
    sheet.save(out)
    print(f'Saved {out}')


if __name__ == '__main__':
    main()
