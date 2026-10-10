"""
Cut a city out of Geofabrik regional extracts, for osm_local.py (the local Overpass stand-in).

Downloads each region in city['osm_pbf'] once to D:\\QGIS\\osm_basemap\\pbf\\<region>-latest.osm.pbf
(skipped if present; --refresh to download again), then writes
D:\\QGIS\\osm_basemap\\pbf\\<city>_<region>.osm.pbf: every node inside the city bbox + margin,
every way with a node there, every relation with such a member, completed with all the
nodes, ways and member ways they reference (so polygons and long ways are whole).

Usage:
    python make_osm_extract.py --city amsterdam
Then run the fetchers with OSM_LOCAL_PBF set to the printed file list (';'-separated).
"""
import argparse
import time
from pathlib import Path

import osmium
import requests

from cities import get_city, add_city_argument

PBF_DIR = Path(r'D:\QGIS\osm_basemap\pbf')
GEOFABRIK = 'https://download.geofabrik.de/{}-latest.osm.pbf'
HEADERS = {'User-Agent': 'transit-frequency-map/1.0 (+https://github.com/jacgeborys/transit-frequency)'}
MARGIN = 0.01  # deg around the bbox (~1 km)


def download(region, refresh=False):
    f = PBF_DIR / f"{region.split('/')[-1]}-latest.osm.pbf"
    if f.exists() and not refresh:
        print(f"{f.name}: present ({f.stat().st_size / 1e6:.0f} MB)")
        return f
    PBF_DIR.mkdir(parents=True, exist_ok=True)
    tmp = f.with_suffix('.part')
    print(f"Downloading {GEOFABRIK.format(region)} ...", flush=True)
    with requests.get(GEOFABRIK.format(region), headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(tmp, 'wb') as out:
            for chunk in r.iter_content(1 << 20):
                out.write(chunk)
    tmp.replace(f)
    print(f"  {f.stat().st_size / 1e6:.0f} MB")
    return f


def extract(src, bbox, out):
    w, s = bbox['west'] - MARGIN, bbox['south'] - MARGIN
    e, n = bbox['east'] + MARGIN, bbox['north'] + MARGIN
    t0 = time.time()
    keep = osmium.IdTracker()
    counts = [0, 0, 0]
    with osmium.BackReferenceWriter(str(out), ref_src=str(src), overwrite=True,
                                    remove_tags=False) as writer:
        for o in osmium.FileProcessor(str(src)):
            if o.is_node():
                loc = o.location
                if loc.valid() and w <= loc.lon <= e and s <= loc.lat <= n:
                    keep.add_node(o.id)
                    writer.add(o)
                    counts[0] += 1
            elif o.is_way():
                if keep.contains_any_references(o):
                    keep.add_way(o.id)
                    writer.add(o)
                    counts[1] += 1
            elif o.is_relation():
                if keep.contains_any_references(o):
                    writer.add(o)
                    counts[2] += 1
    print(f"  {out.name}: {counts[0]:,} nodes, {counts[1]:,} ways, {counts[2]:,} relations in bbox "
          f"(+ referenced objects), {out.stat().st_size / 1e6:.0f} MB, {time.time() - t0:.0f}s")


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    ap.add_argument('--refresh', action='store_true', help='Download the regions again')
    args = ap.parse_args()
    city = get_city(args.city)
    outs = []
    for region in city['osm_pbf']:
        src = download(region, args.refresh)
        out = PBF_DIR / f"{city['key']}_{region.split('/')[-1]}.osm.pbf"
        if out.exists() and out.stat().st_mtime > src.stat().st_mtime and not args.refresh:
            print(f"  {out.name}: present, newer than {src.name}")
        else:
            extract(src, city['bbox'], out)
        outs.append(str(out))
    print(f"\nOSM_LOCAL_PBF={';'.join(outs)}")


if __name__ == '__main__':
    main()
