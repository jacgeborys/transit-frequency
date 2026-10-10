"""
Whole pipeline for one city, the same way for every city: GTFS -> trip counts -> OSM extract ->
basemap -> walking network -> isochrones -> coverage (raster) -> building values -> poster.

    python run_city.py --city zurich --date 20261014 --detach      # usual call (hidden, survives the session)
    python run_city.py --city amsterdam --date 20261014 --from iso --final

Steps (names for --from / --to): gtfs, counts, osm, basemap, network, iso, coverage, buildings,
render, final (300 dpi, only with --final), cleanup (only with --cleanup-osm).
A step is skipped when its output exists and is newer than its inputs (make-like), so a rerun
after an interruption continues where it stopped; --from STEP forces that step and all later ones.

Needs in cities.py: bbox, crs_metric, gtfs (+ gtfs_merge), osm_pbf (Geofabrik regions),
template/render_frame for the poster; header texts in render_poster.py (POSTER).
OSM data comes from Geofabrik extracts (make_osm_extract.py) answered locally (osm_local.py via
OSM_LOCAL_PBF), so nothing is sent to Overpass. Milestones: png/log_buildings_final.txt;
step logs: <data folder>/logs/<step>.txt. Coverage uses 04_coverage_raster.py (same result as
04_create_coverage_map.py, which stays available as a check: --vector).
"""
import argparse
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from cities import get_city, add_city_argument

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parent
BASEMAP_FETCHER = Path(r'D:\QGIS\osm_basemap\fetch_osm_basemap.py')
PBF_DIR = Path(r'D:\QGIS\osm_basemap\pbf')
LOG = PROJECT / 'png' / 'log_buildings_final.txt'
STEPS = ['gtfs', 'counts', 'osm', 'basemap', 'network', 'iso', 'coverage', 'buildings',
         'render', 'final', 'cleanup']
PY = sys.executable


def milestone(city, msg):
    line = f"{datetime.now():%d.%m.%Y %H:%M:%S} {city['key'].upper()} {msg}"
    print(line, flush=True)
    with open(LOG, 'a', encoding='utf-8') as f:
        f.write(line + '\n')


def newer(outputs, inputs):
    """True if every output exists and is newer than every existing input."""
    outs = [Path(o) for o in outputs]
    if not all(o.exists() for o in outs):
        return False
    t_in = max((Path(i).stat().st_mtime for i in inputs if Path(i).exists()), default=0)
    return min(o.stat().st_mtime for o in outs) > t_in


def run(cmd, log, env=None):
    log.parent.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    with open(log, 'w', encoding='utf-8') as f:
        rc = subprocess.call([PY, '-u', *map(str, cmd)], cwd=HERE, stdout=f, stderr=subprocess.STDOUT,
                             env={**os.environ, 'PYTHONIOENCODING': 'utf-8', **(env or {})})
    if rc != 0:
        raise RuntimeError(f"{Path(str(cmd[0])).name} failed (exit {rc}), see {log}")
    return time.time() - t0


def detach(argv):
    """Relaunch this command hidden and outside the calling session's process tree (WMI)."""
    args = [a for a in argv if a != '--detach']
    out = PROJECT / 'png' / f"run_city_{datetime.now():%Y%m%d_%H%M%S}.txt"
    inner = f'"{PY}" -u "{Path(__file__).resolve()}" ' + ' '.join(f'"{a}"' for a in args)
    cmdline = f'cmd.exe /c "{inner} > "{out}" 2>&1"'
    ps = ("$si = New-CimInstance -ClassName Win32_ProcessStartup -ClientOnly -Property @{ShowWindow=[uint16]0}; "
          "$r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{CommandLine="
          f"'{cmdline.replace(chr(39), chr(39) * 2)}'; CurrentDirectory='{HERE}'; ProcessStartupInformation=$si}}; "
          "$r.ProcessId")
    pid = subprocess.check_output(['powershell', '-NoProfile', '-Command', ps], text=True).strip()
    print(f"Started hidden (pid {pid}); output: {out}; milestones: {LOG}")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    ap.add_argument('--date', required=True, help='Analysis day YYYYMMDD (a school-term weekday)')
    ap.add_argument('--data', help='GTFS data folder (default: today\'s, downloaded by step gtfs)')
    ap.add_argument('--from', dest='from_step', choices=STEPS, help='Force this step and all later ones')
    ap.add_argument('--to', dest='to_step', choices=STEPS, default='cleanup', help='Stop after this step')
    ap.add_argument('--final', action='store_true', help='Also render the 300 dpi poster into png/')
    ap.add_argument('--vector', action='store_true', help='Coverage with the vector 04 (slow; for checks)')
    ap.add_argument('--cleanup-osm', action='store_true',
                    help='At the end, delete the Geofabrik regional files of this city (keeps the city extract)')
    ap.add_argument('--detach', action='store_true', help='Run hidden in the background and return')
    args = ap.parse_args()
    if args.detach:
        return detach(sys.argv[1:])

    city = get_city(args.city)
    key = city['key']
    date = args.date
    poster_date = f"{date[6:8]}.{date[4:6]}.{date[:4]}"
    first = STEPS.index(args.from_step) if args.from_step else None
    last = STEPS.index(args.to_step)
    regions = [r.split('/')[-1] for r in city.get('osm_pbf', [])]
    extracts = [PBF_DIR / f"{key}_{r}.osm.pbf" for r in regions]
    env = {'OSM_LOCAL_PBF': ';'.join(map(str, extracts))}
    osm_dir, net = city['osm_dir'], city['network_dir'] / 'walking_network.pkl'
    data = Path(args.data) if args.data else city['data_dir'] / datetime.now().strftime('%Y_%m_%d')
    logs = data / 'logs'

    def todo(step, outputs=(), inputs=()):
        i = STEPS.index(step)
        if i > last:
            return False
        if first is not None:
            return i >= first
        return not (outputs and newer(outputs, inputs))

    milestone(city, f"run_city start (date {date}, data {data.name})")
    t_all = time.time()
    try:
        if todo('gtfs', [data / 'stops.txt']):
            dt = run(['00_download_gtfs.py', '--city', key], city['data_dir'] / f"log_00_{data.name}.txt")
            milestone(city, f"gtfs done ({dt / 60:.1f} min)")
        counts = data / 'stops_trip_count.csv'
        if todo('counts', [counts], [data / 'stop_times.txt']):
            dt = run(['01_calculate_trip_counts.py', '--city', key, date, data], logs / '01.txt')
            milestone(city, f"counts done ({dt / 60:.1f} min)")
        if todo('osm', extracts):
            if not regions:
                raise RuntimeError("cities.py has no 'osm_pbf' (Geofabrik regions) for this city")
            dt = run(['make_osm_extract.py', '--city', key], logs / 'extract.txt')
            milestone(city, f"osm extract done ({dt / 60:.1f} min)")
        if todo('basemap', [osm_dir / 'buildings.gpkg', osm_dir / 'underground_ids.txt'], extracts):
            b = city['bbox']
            dt = run([BASEMAP_FETCHER, '--bbox', f"{b['south']},{b['west']},{b['north']},{b['east']}",
                      '--name', key, '--crs', city['crs_metric']], logs / 'basemap.txt', env)
            dt += run(['drop_underground_buildings.py', '--city', key], logs / 'underground.txt', env)
            milestone(city, f"basemap done ({dt / 60:.1f} min)")
        if todo('network', [net], extracts):
            if first is not None and net.exists():
                net.unlink()   # forced: 02 refuses to overwrite
            dt = run(['02_fetch_walking_network.py', '--city', key], logs / '02.txt', env)
            milestone(city, f"network done ({dt / 60:.1f} min)")
        iso = [data / 'isochrones_gates.gpkg', data / 'isochrones_gates_residents.gpkg']
        if todo('iso', iso, [counts, net, osm_dir / 'buildings.gpkg', osm_dir / 'barriers.gpkg']):
            dt = run(['03_generate_isochrones_local.py', '--city', key, '--gates', data], logs / '03.txt')
            milestone(city, f"isochrones done ({dt / 60:.1f} min)")
        cov = [data / 'coverage_map_gates.gpkg', data / 'coverage_map_gates_residents.gpkg']
        if todo('coverage', cov, iso):
            dt = 0
            for v, out in zip(('gates', 'gates_residents'), cov):
                cmd = (['04_create_coverage_map.py', '--city', key, '--variant', v, '--out', out, data]
                       if args.vector else
                       ['04_coverage_raster.py', '--city', key, '--variant', v, '--out', out, data])
                dt += run(cmd, logs / f"04_{v}.txt")
            milestone(city, f"coverage done ({'vector' if args.vector else 'raster'}, {dt / 60:.1f} min)")
        bld = data / 'buildings_gates.gpkg'
        if todo('buildings', [bld], cov):
            dt = run(['05_building_values.py', '--city', key, '--variant', 'gates', data], logs / '05.txt')
            milestone(city, f"building values done ({dt / 60:.1f} min)")
        preview = PROJECT / 'png' / 'previews' / f"{key}_poster_150.png"
        if todo('render', [preview], [bld] + cov):
            dt = run(['render_poster.py', '--city', key, '--date', poster_date, '--dpi', '150',
                      '--data', data, '--out', preview], logs / 'render_150.txt')
            milestone(city, f"preview done ({dt / 60:.1f} min): {preview}")
        if args.final and todo('final', ['_never_'], []):
            y, m, d = date[:4], date[4:6], date[6:8]
            out = PROJECT / 'png' / f"how_many_rides_in_5_mins_{key}_{y}_{m}_{d}_dark_bmy_bright_hq.png"
            dt = run(['render_poster.py', '--city', key, '--date', poster_date, '--dpi', '300',
                      '--data', data, '--out', out], logs / 'render_300.txt')
            milestone(city, f"300 dpi final done ({dt / 60:.1f} min): {out}")
        if args.cleanup_osm and todo('cleanup', ['_never_'], []):
            for r in regions:
                f = PBF_DIR / f"{r}-latest.osm.pbf"
                if f.exists():
                    f.unlink()
                    milestone(city, f"removed {f.name} (city extract kept)")
    except Exception as e:
        milestone(city, f"FAILED: {e}")
        sys.exit(1)
    milestone(city, f"run_city ALL DONE ({(time.time() - t_all) / 60:.0f} min)")


if __name__ == '__main__':
    main()
