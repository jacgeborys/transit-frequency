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
    '--road-edge-mm', '0.1', '--road-edge-rgb', '70,72,82',
    '--recolor', 'water=26,46,74', '--recolor', 'roads=34,35,42',
    '--recolor', 'railways=58,58,66', '--line-scale', 'railways=0.6',
    '--recolor', 'buildings=45,46,52',
    '--building-outline-mm', '0.05', '--building-outline-rgb', '95,96,105', '--building-outline-spill', '0.35',
    '--page-rgb', '0,0,0', '--ink-rgb', '225,225,232',
    '--palette', 'bmy_dark', '--min-lightness', '34', '--palette-extend', '#fff38a,#fffbd6',
    '--scale-bar',
]

PL = {'headline': 'Ile odjazdów masz w zasięgu 5 minut pieszo?',
      'subline': 'dzień powszedni · środa {date}'}
DE = {'headline': 'Wie viele Abfahrten erreichst du in 5 Minuten zu Fuß?',
      'subline': 'Werktag · Mittwoch, {date}', 'legend_title': 'Abfahrten/Tag'}
EN = {'headline': 'How many departures within a 5-minute walk?',
      'subline': 'weekday · Wednesday {date}', 'legend_title': 'Departures/day'}
NL = {'headline': 'Hoeveel vertrekken binnen 5 minuten lopen?',
      'subline': 'werkdag · woensdag {date}', 'legend_title': 'Vertrekken/dag'}

POSTER = {
    'warsaw': {'city': 'WARSZAWA', **PL, 'extra': ['--extend-left-m', '1000', '--extend-right-m', '1000']},
    'krakow': {'city': 'KRAKÓW', **PL},
    'poznan': {'city': 'POZNAŃ', **PL},
    'gdansk': {'city': 'GDAŃSK', **PL},
    'lublin': {'city': 'LUBLIN', **PL},
    'berlin': {'city': 'BERLIN', **DE},
    'amsterdam': {'city': 'AMSTERDAM', **NL},
    'manhattan': {'city': 'NEW YORK', **EN, 'subline': 'weekday · Wednesday, October 14, 2026'},
}


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
