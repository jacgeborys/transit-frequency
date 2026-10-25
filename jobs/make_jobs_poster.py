"""
Jobs reachable by public transport, as a dark poster in the style of the transit-frequency maps.

Data (read only): D:/QGIS/ceny_mieszkan/access/output/access_<scenario>_cells_partial.csv, R5 door-to-door
routing (walk + wait + ride + transfers) on the Warsaw GTFS of Wed 7.10.2026, median over every departure
minute of the window, per 250 m origin cell (EPSG:2180, cell id = (x // 250) * 100000 + y // 250).
Jobs = BDOT10k floor area per job by building category (ceny_mieszkan/access/build_inputs.py).
Cells: jobs/data/access_<scenario>_all.csv from travel_times_all.py (every built-up cell, ~10k) when present,
else ceny_mieszkan's run (only the 3,255 cells with priced flats). Every building takes its own cell's value, or
the nearest routed cell's within FILL_M; other buildings stay basemap grey.

Usage (from jobs/):
    python make_jobs_poster.py [--scenario am_peak] [--measure jobs_30] [--dpi 150]
    -> jobs/data/buildings_<measure>_<scenario>.gpkg, jobs/renders/warsaw_<measure>_<scenario>_<dpi>.png
"""
import argparse
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from scipy.spatial import cKDTree

HERE = Path(__file__).resolve().parent
SCRIPT = HERE.parent / 'script'
sys.path.insert(0, str(SCRIPT))
from cities import get_city  # noqa: E402
import render_poster  # noqa: E402  (STYLE: the poster look)

ACCESS = Path('D:/QGIS/ceny_mieszkan/access/output')
CELL = 250
FILL_M = 375                  # a building without a routed cell borrows the nearest one this close
EXCLUDED_BUILDING_TYPES = {'roof', 'carport', 'service'}
BIG_BUILDING_M2 = 5000
# thousands of jobs; 25 classes like the frequency posters
BREAKS = '1,2,5,10,15,20,30,40,50,60,75,100,125,150,175,200,250,300,350,400,450,500,550,600,650'
TICKS = '1,10,50,100,200,300,400,500,650'
SCENARIOS = {'am_peak': 'środa {date}, szczyt poranny 7–9', 'midday': 'środa {date}, 11–13',
             'evening': 'środa {date}, wieczór 19–21', 'saturday': 'sobota 10.10.2026, 11–13'}
MEASURES = {'jobs_30': ('Ile miejsc pracy dojedziesz komunikacją w 30 minut?', 'Miejsca pracy (tys.)'),
            'jobs_45': ('Ile miejsc pracy dojedziesz komunikacją w 45 minut?', 'Miejsca pracy (tys.)')}


def building_values(city, cells, measure):
    crs = city['crs_metric']
    b = gpd.read_file(city['osm_dir'] / 'buildings.gpkg')
    if 'building' in b.columns:
        b = b[~b['building'].isin(EXCLUDED_BUILDING_TYPES)]
    b = b[b.geometry.notna() & b.geom_type.isin(['Polygon', 'MultiPolygon'])].to_crs(crs).reset_index(drop=True)
    p = b.geometry.representative_point()
    cid = (p.x // CELL).astype(int) * 100000 + (p.y // CELL).astype(int)
    val = cid.map(cells.set_index('from_id')[measure])
    # buildings in unrouted cells: nearest routed cell centre within FILL_M
    cx = (cells['from_id'] // 100000 + 0.5) * CELL
    cy = (cells['from_id'] % 100000 + 0.5) * CELL
    d, k = cKDTree(np.column_stack([cx, cy])).query(np.column_stack([p.x, p.y]))
    near = cells[measure].to_numpy()[k]
    fill = val.isna() & (d <= FILL_M)
    val[fill] = near[fill.to_numpy()]
    out = gpd.GeoDataFrame({'osm_id': b['osm_id'], 'value': val}, geometry=b.geometry, crs=crs)
    out = out[out['value'].notna() & (out['value'] > 0)]
    out['max_trips'] = np.maximum(1, np.round(out['value'] / 1000)).astype(int)   # thousands
    out['residents_only'] = False
    out['big'] = out.geometry.area > BIG_BUILDING_M2
    print(f"buildings: {len(b):,}; coloured {len(out):,} ({fill.sum():,} from a neighbouring cell)")
    return out


def cell_polygons(cells, measure, crs):
    """250 m squares as the 'coverage' layer (only used inside big buildings; no fill on the poster)."""
    import shapely
    x0 = (cells['from_id'] // 100000) * CELL
    y0 = (cells['from_id'] % 100000) * CELL
    g = shapely.box(x0, y0, x0 + CELL, y0 + CELL)
    v = np.maximum(1, np.round(cells[measure] / 1000)).astype(int)
    return gpd.GeoDataFrame({'deduped_trips': v, 'unique_routes': 0, 'area_ha': 6.25}, geometry=g, crs=crs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--scenario', default='am_peak', choices=SCENARIOS)
    ap.add_argument('--measure', default='jobs_30', choices=MEASURES)
    ap.add_argument('--dpi', default='150')
    args, passthrough = ap.parse_known_args()
    city = get_city('warsaw')
    crs = city['crs_metric']
    full = HERE / 'data' / f'access_{args.scenario}_all.csv'   # travel_times_all.py: every built-up cell
    cells = pd.read_csv(full if full.exists() else ACCESS / f'access_{args.scenario}_cells_partial.csv')
    print(f"cells: {len(cells):,} from {full.name if full.exists() else 'ceny_mieszkan (priced cells only)'}")
    (HERE / 'data').mkdir(exist_ok=True)
    (HERE / 'renders').mkdir(exist_ok=True)
    tag = f'{args.measure}_{args.scenario}'
    bld_file = HERE / 'data' / f'buildings_{tag}.gpkg'
    cov_file = HERE / 'data' / f'cells_{tag}.gpkg'
    building_values(city, cells, args.measure).to_crs(4326).to_file(bld_file, driver='GPKG')
    cell_polygons(cells, args.measure, crs).to_crs(4326).to_file(cov_file, driver='GPKG')

    headline, legend = MEASURES[args.measure]
    out = HERE / 'renders' / f'warsaw_{tag}_{args.dpi}.png'
    cmd = [sys.executable, 'render_qgis_style.py', '--city', 'warsaw',
           '--coverage', str(cov_file), '--buildings', str(bld_file),
           '--date', '07.10.2026', '--dpi', args.dpi, *render_poster.STYLE,
           '--headline-city', 'WARSZAWA', '--headline', headline,
           '--subline', SCENARIOS[args.scenario] + ' · od drzwi do drzwi: dojście, czekanie, jazda, przesiadki',
           '--legend-title', legend, '--breaks', BREAKS, '--legend-ticks', TICKS,
           '--coverage-fade', '0', '--out', str(out), *passthrough]
    print(' '.join(cmd[1:]), flush=True)
    sys.exit(subprocess.call(cmd, cwd=SCRIPT))


if __name__ == '__main__':
    main()
