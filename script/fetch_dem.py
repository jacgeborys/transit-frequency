"""
Terrain model (DEM) for slope-aware walking: D:\\QGIS\\osm_basemap\\dem\\<city>.tif in the city's
metric CRS, covering its bbox. 03 uses it when the city has 'hills': True.

Sources (city['dem']):
  'swissalti3d'  swisstopo swissALTI3D, 2 m, 1 km tiles via the geo.admin.ch STAC API (newest
                 year per tile), merged.
  'usgs13'       USGS 3DEP 1/3 arc-second (~10 m) seamless DEM, 1x1 degree cloud-optimised GeoTIFFs
                 on AWS; only the bbox window is read (no full download), reprojected to 10 m.
Both are bare-earth terrain models (no buildings, no bridges).

Usage:
    python fetch_dem.py --city zurich
"""
import argparse
import math
import re
from pathlib import Path

import numpy as np
import rasterio
import requests
from rasterio.merge import merge
from rasterio.warp import Resampling, calculate_default_transform, reproject

from cities import get_city, add_city_argument

UA = {'User-Agent': 'transit-frequency-map/1.0 (+https://github.com/jacgeborys/transit-frequency)'}
STAC = 'https://data.geo.admin.ch/api/stac/v0.9/collections/ch.swisstopo.swissalti3d/items'
USGS13 = 'https://prd-tnm.s3.amazonaws.com/StagedProducts/Elevation/13/TIFF/current/{t}/USGS_13_{t}.tif'


def dem_path(city):
    return Path(r'D:\QGIS\osm_basemap\dem') / f"{city['key']}.tif"


def swissalti3d(b, out, cache):
    cache.mkdir(parents=True, exist_ok=True)
    url = f"{STAC}?bbox={b['west']},{b['south']},{b['east']},{b['north']}&limit=100"
    tiles = {}   # tile key -> (year, href): newest year wins
    while url:
        d = requests.get(url, headers=UA, timeout=60).json()
        for f in d['features']:
            for a in f['assets'].values():
                h = a['href']
                m = re.search(r'swissalti3d_(\d{4})_(\d{4}-\d{4})_2_2056', h)
                if m and h.endswith('.tif') and int(m.group(1)) >= tiles.get(m.group(2), (0, ''))[0]:
                    tiles[m.group(2)] = (int(m.group(1)), h)
        url = next((l['href'] for l in d.get('links', []) if l.get('rel') == 'next'), None)
    print(f"swissALTI3D: {len(tiles)} tiles of 1 km at 2 m")
    files = []
    for i, (key, (year, h)) in enumerate(sorted(tiles.items()), 1):
        f = cache / h.rsplit('/', 1)[1]
        if not f.exists():
            r = requests.get(h, headers=UA, timeout=120)
            r.raise_for_status()
            f.write_bytes(r.content)
        files.append(f)
        if i % 25 == 0:
            print(f"  {i}/{len(tiles)}", flush=True)
    srcs = [rasterio.open(f) for f in files]
    arr, transform = merge(srcs)
    meta = srcs[0].meta.copy()
    meta.update(height=arr.shape[1], width=arr.shape[2], transform=transform, compress='deflate',
                tiled=True, nodata=srcs[0].nodata)
    for s in srcs:
        s.close()
    with rasterio.open(out, 'w', **meta) as dst:
        dst.write(arr)


def usgs13(b, out, crs, res=10.0):
    tiles = {f"n{math.ceil(lat):02d}w{math.ceil(-lon):03d}"
             for lat in (b['south'], b['north']) for lon in (b['west'], b['east'])}
    print(f"USGS 1/3 arc-second: tiles {sorted(tiles)} (window read over HTTP)")
    srcs = [rasterio.open('/vsicurl/' + USGS13.format(t=t)) for t in sorted(tiles)]
    arr, transform = merge(srcs, bounds=(b['west'], b['south'], b['east'], b['north']))
    src_crs, nodata = srcs[0].crs, srcs[0].nodata
    for s in srcs:
        s.close()
    h, w = arr.shape[1:]
    dt, dw, dh = calculate_default_transform(src_crs, crs, w, h, b['west'], b['south'],
                                             b['east'], b['north'], resolution=res)
    dst = np.full((dh, dw), np.nan, dtype='float32')
    reproject(arr[0].astype('float32'), dst, src_transform=transform, src_crs=src_crs,
              dst_transform=dt, dst_crs=crs, src_nodata=nodata, dst_nodata=np.nan,
              resampling=Resampling.bilinear)
    with rasterio.open(out, 'w', driver='GTiff', height=dh, width=dw, count=1, dtype='float32',
                       crs=crs, transform=dt, nodata=np.nan, compress='deflate', tiled=True) as d:
        d.write(dst, 1)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    args = ap.parse_args()
    city = get_city(args.city)
    out = dem_path(city)   # own folder: run_city sets the basemap folder aside on rebuilds
    out.parent.mkdir(parents=True, exist_ok=True)
    src = city.get('dem')
    if src == 'swissalti3d':
        swissalti3d(city['bbox'], out, Path(r'D:\QGIS\osm_basemap\dem_cache\swissalti3d'))
    elif src == 'usgs13':
        usgs13(city['bbox'], out, city['crs_metric'])
    else:
        raise SystemExit(f"cities.py: no 'dem' source for {city['name']} (swissalti3d, usgs13)")
    with rasterio.open(out) as d:
        a = d.read(1, masked=True)
        print(f"{out}: {d.width} x {d.height} px at {d.res[0]:.1f} m, {d.crs}, "
              f"elevation {float(a.min()):.0f}..{float(a.max()):.0f} m")


if __name__ == '__main__':
    main()
