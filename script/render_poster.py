"""
Render the current product: the dark "bmy_bright" poster of a city.

Wraps render_qgis_style.py with the agreed style (2026-10-09): dark page, bmy_dark palette
with a lightness floor and a brighter top, coverage fill 0.42, buildings in full class
colours, Bahnschrift poster header (big city name + question + date line) and a km scale bar.

Usage:
    python render_poster.py --city warsaw                 # 300 dpi -> png/..._dark_bmy_bright_hq.png
    python render_poster.py --city berlin --dpi 150 --out ../png/previews/test.png
    python render_poster.py --city manhattan --data ../_data/manhattan/2026_10_14

Header texts per city live in POSTER below (language follows the city). Cities without a
QGIS layout of their own use their 'template' city's layout (see cities.py).
"""
import argparse
import subprocess
import sys
from pathlib import Path

from cities import get_city, PROJECT_DIR

# Shared dark style (see README "Dark mode" / "Posters")
STYLE = [
    '--restricted-buildings',
    '--bg-rgb', '0,0,0', '--green-rgb', '30,44,34', '--forest-rgb', '28,50,37',
    '--uncovered-rgb', '58,58,64', '--coverage-fade', '0.42', '--min-fill-lightness', '7',
    '--building-shade', '1.0', '--building-saturation', '1.1',
    '--road-edge-mm', '0.06', '--road-edge-rgb', '66,68,80',
    '--recolor', 'water=22,39,63', '--recolor', 'roads=80,82,94',
    '--recolor', 'railways=100,100,112', '--line-scale', 'railways=0.6',
    '--recolor', 'buildings=45,46,52',
    '--building-outline-mm', '0.05', '--building-outline-rgb', '95,96,105', '--building-outline-spill', '0.35',
    '--page-rgb', '0,0,0', '--ink-rgb', '225,225,232',
    '--palette', 'bmy_dark', '--min-lightness', '40', '--min-lightness-step', '1.6', '--palette-extend', '#fff38a,#fffbd6',
    '--scale-bar', '--north-arrow', '--legend-scale-auto',
    '--roads-under-buildings',  # coloured buildings over streets/railways (2026-10-13)
]

PL = {'headline': 'Ile odjazdów masz w zasięgu 5 minut pieszo?',
      'subline': 'dzień powszedni · środa {date}'}
DE = {'headline': 'Wie viele Abfahrten erreichst du in 5 Minuten zu Fuß?',
      'subline': 'Werktag · Mittwoch, {date}', 'legend_title': 'Abfahrten/Tag'}
EN = {'headline': 'How many departures within a 5-minute walk?',
      'subline': 'weekday · Wednesday {date}', 'legend_title': 'Departures/day'}
DE_CH = {'headline': 'Wie viele Abfahrten erreichst du in 5 Minuten zu Fuss?',  # Swiss: no ß
         'subline': 'Werktag · Mittwoch, {date}', 'legend_title': 'Abfahrten/Tag'}
NL = {'headline': 'Hoeveel vertrekken binnen 5 minuten lopen?',
      'subline': 'werkdag · woensdag {date}', 'legend_title': 'Vertrekken/dag'}
FR = {'headline': 'Combien de départs à 5 minutes à pied ?',
      'subline': 'jour ouvré · mercredi {date}', 'legend_title': 'Départs/jour'}

POSTER = {
    'warsaw': {'city': 'WARSZAWA', **PL, 'extra': ['--extend-left-m', '1000', '--extend-right-m', '1000']},
    'krakow': {'city': 'KRAKÓW', **PL},
    'poznan': {'city': 'POZNAŃ', **PL},
    'gdansk': {'city': 'GDAŃSK', **PL},
    'tricity': {'city': 'TRÓJMIASTO', **PL},
    'lublin': {'city': 'LUBLIN', **PL},
    'berlin': {'city': 'BERLIN', **DE},
    'amsterdam': {'city': 'AMSTERDAM', **NL},
    'zurich': {'city': 'ZÜRICH', **DE_CH},
    'paris': {'city': 'PARIS', **FR, 'subline': 'jour ouvré · mercredi 14 octobre 2026'},
    'sanfrancisco': {'city': 'SAN FRANCISCO', **EN, 'subline': 'weekday · Wednesday, October 14, 2026'},
    'manhattan': {'city': 'NEW YORK', **EN, 'subline': 'weekday · Wednesday, October 14, 2026'},
    'newyork': {'city': 'NEW YORK', **EN, 'subline': 'weekday · Wednesday, October 14, 2026'},
    'boston': {'city': 'BOSTON', **EN, 'subline': 'weekday · Wednesday, October 14, 2026'},
}


def style_value(flag, style=STYLE):
    """Value following `flag` in STYLE (all occurrences for repeatable flags like --recolor)."""
    vals = [style[i + 1] for i, s in enumerate(style) if s == flag]
    return vals if flag in ('--recolor', '--line-scale') else (vals[0] if vals else None)


def poster_colours(city_key):
    """Poster colours as 0-255 RGB tuples, for other renderers (3d/): building classes
    [(lower, upper, rgb)] (QGIS semantics: lower < max_trips <= upper), uncovered buildings,
    background, green, forest and each --recolor layer; coverage_classes [(lower, upper, rgb, alpha)]
    = the coverage fill as composited on the poster (draw rgb at alpha over the background). Same code path as render_qgis_style."""
    import render_qgis_style as rq
    city = get_city(city_key)
    tmpl = city if 'qgis_layout' in city else get_city(city.get('template', 'krakow'))
    root = rq.load_project(rq.PROJECT_FILE)
    layers = {ml.find('id').text: ml for ml in root.iter('maplayer')}
    group = rq.find_group(rq.find_group(root.find('layer-tree-group'), 'Tlo'), tmpl['qgis_group'])
    cov = next(rq.parse_layer(layers[i], rq.PROJECT_DIR) for i in rq.visible_layer_ids(group)
               if 'coverage_map' in rq.parse_layer(layers[i], rq.PROJECT_DIR)['path'].name)
    r = cov['renderer']
    extend = style_value('--palette-extend')
    rq.apply_palette(r, style_value('--palette'), extend.split(',') if extend else None,
                     float(style_value('--min-lightness')), float(style_value('--min-lightness-step')))
    br = rq.building_renderer(r, float(style_value('--building-shade')),
                              float(style_value('--building-saturation')))
    # Coverage fill as on the poster: lightness floor, then alpha = class x symbol x layer x fade
    fade = float(style_value('--coverage-fade'))
    bg = tuple(int(v) / 255 for v in style_value('--bg-rgb').split(','))
    rq.floor_fill_lightness(r, cov['opacity'] * fade, bg, float(style_value('--min-fill-lightness')))
    coverage = []
    for lo, hi, sym, _, _ in r['ranges']:
        fill = next(p for c, p in r['symbols'][sym]['layers'] if c == 'SimpleFill')
        c = rq.parse_color(fill['color'])
        coverage.append((lo, hi, tuple(int(round(v * 255)) for v in c[:3]),
                         c[3] * r['symbols'][sym]['alpha'] * cov['opacity'] * fade))
    classes = []
    for lo, hi, sym, _, _ in br['ranges']:
        fill = next(p for c, p in br['symbols'][sym]['layers'] if c == 'SimpleFill')
        classes.append((lo, hi, tuple(int(v) for v in fill['color'].split(',')[:3])))
    rgb = lambda s: tuple(int(v) for v in s.split(','))  # noqa: E731
    out = {'classes': classes, 'coverage_classes': coverage, 'uncovered': rgb(style_value('--uncovered-rgb')),
           'background': rgb(style_value('--bg-rgb')), 'green': rgb(style_value('--green-rgb')),
           'forest': rgb(style_value('--forest-rgb'))}
    for spec in style_value('--recolor'):
        stem, c = spec.split('=')
        out[stem] = rgb(c)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--city', required=True)
    ap.add_argument('--date', default='07.10.2026', help='Shown in the date line (DD.MM.YYYY)')
    ap.add_argument('--dpi', default='300')
    ap.add_argument('--data', help='Data folder (default: latest with coverage_map_gates.gpkg)')
    ap.add_argument('--out', help='Output PNG (default: png/how_many_rides_in_5_mins[_city]_<date>_dark_bmy_bright_hq.png)')
    args, passthrough = ap.parse_known_args()  # anything else goes to render_qgis_style.py

    city = get_city(args.city)
    poster = POSTER.get(city['key'], {'city': city['name'].upper(), **EN})
    data = Path(args.data) if args.data else sorted(
        d for d in city['data_dir'].iterdir() if (d / 'coverage_map_gates.gpkg').exists())[-1]
    d, m, y = args.date.split('.')
    suffix = '' if city['key'] == 'warsaw' else f"_{city['key']}"
    out = Path(args.out) if args.out else \
        PROJECT_DIR / 'png' / f'how_many_rides_in_5_mins{suffix}_{y}_{m}_{d}_dark_bmy_bright_hq.png'

    cmd = [sys.executable, 'render_qgis_style.py', '--city', city['key'],
           '--coverage', str(data / 'coverage_map_gates.gpkg'),
           '--buildings', str(data / 'buildings_gates.gpkg'),
           '--date', args.date, '--dpi', args.dpi, *STYLE,
           '--headline-city', poster['city'], '--headline', poster['headline'],
           '--subline', poster['subline'], *poster.get('extra', []), '--out', str(out)]
    if poster.get('legend_title'):
        cmd += ['--legend-title', poster['legend_title']]
    cmd += passthrough
    print(' '.join(f'"{c}"' if ' ' in c else c for c in cmd[1:]), flush=True)
    sys.exit(subprocess.call(cmd, cwd=Path(__file__).resolve().parent))


if __name__ == '__main__':
    main()
