"""
Fetch Walking Network for a city.
Downloads the OSM walking network via direct Overpass queries (tiled),
then builds a NetworkX graph with OSMnx.

Usage:
    python 02_fetch_walking_network.py --city warsaw
    python 02_fetch_walking_network.py --city gdansk
"""
import argparse
import sys
import json
import warnings
import osmnx as ox
import networkx as nx
import pickle
import time
import requests
from pathlib import Path
from datetime import datetime
from shapely.geometry import box

from cities import get_city, add_city_argument
import overpass_polite

# Requests go through overpass_polite (slot-aware, backs off, drops failing mirrors)
ATTEMPTS_PER_TILE = 6
HEADERS = {'User-Agent': 'QGIS-walking-network/1.0', 'Accept': '*/*'}
RETRY_ROUNDS = 2  # extra passes over failed tiles before giving up

WALK_QUERY = """[out:json][timeout:180];
(way["highway"]["area"!~"yes"]["highway"!~"abandoned|bus_guideway|construction|cycleway|motor|no|planned|platform|proposed|raceway|razed"]["foot"!~"no"]["service"!~"private"]({bbox});>;);out;"""


def create_tiles(bbox, n=4):
    """Split bbox into n x n tiles."""
    lat_step = (bbox['north'] - bbox['south']) / n
    lon_step = (bbox['east'] - bbox['west']) / n
    tiles = []
    for i in range(n):
        for j in range(n):
            tiles.append({
                'south': bbox['south'] + i * lat_step,
                'north': bbox['south'] + (i + 1) * lat_step,
                'west': bbox['west'] + j * lon_step,
                'east': bbox['west'] + (j + 1) * lon_step,
                'id': f"{i}_{j}",
            })
    return tiles


def split_against(tile, ob):
    """Parts of `tile` outside bbox `ob` (0-4 strips, ids <tile>_s<k>), or None if no overlap."""
    s_, n_ = max(tile['south'], ob['south']), min(tile['north'], ob['north'])
    w_, e_ = max(tile['west'], ob['west']), min(tile['east'], ob['east'])
    if s_ >= n_ or w_ >= e_:
        return None
    strips = []
    for k, r in enumerate(((tile['south'], tile['west'], s_, tile['east']),
                           (n_, tile['west'], tile['north'], tile['east']),
                           (s_, tile['west'], n_, w_),
                           (s_, e_, n_, tile['east']))):
        if r[2] - r[0] > 1e-9 and r[3] - r[1] > 1e-9:
            strips.append({'south': r[0], 'west': r[1], 'north': r[2], 'east': r[3],
                           'id': f"{tile['id']}_s{k}"})
    return strips


def fetch_tile_json(tile, cache_dir):
    """Fetch walking network JSON for a single tile, with caching and retries."""
    cache_file = cache_dir / f"tile_{tile['id']}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding='utf-8'))

    bbox_str = f"{tile['south']},{tile['west']},{tile['north']},{tile['east']}"
    query = WALK_QUERY.format(bbox=bbox_str)

    data = overpass_polite.post(query, HEADERS, timeout=300, attempts=ATTEMPTS_PER_TILE)
    if data is not None:
        cache_file.write_text(json.dumps(data), encoding='utf-8')
    return data


def build_graph_from_jsons(jsons):
    """Build a walking NetworkX graph from Overpass JSON responses."""
    nodes = {}
    ways = []

    for data in jsons:
        for elem in data.get('elements', []):
            if elem['type'] == 'node':
                nodes[elem['id']] = (elem['lon'], elem['lat'])
            elif elem['type'] == 'way':
                ways.append(elem)

    G = nx.MultiDiGraph()
    for nid, (lon, lat) in nodes.items():
        G.add_node(nid, x=lon, y=lat)

    for way in ways:
        tags = way.get('tags', {})
        way_nodes = way.get('nodes', [])
        highway = tags.get('highway', '')

        for i in range(len(way_nodes) - 1):
            u, v = way_nodes[i], way_nodes[i + 1]
            if u not in nodes or v not in nodes:
                continue
            lon_u, lat_u = nodes[u]
            lon_v, lat_v = nodes[v]
            # Haversine distance in meters
            from math import radians, sin, cos, sqrt, atan2
            R = 6371000
            dlat = radians(lat_v - lat_u)
            dlon = radians(lon_v - lon_u)
            a = sin(dlat/2)**2 + cos(radians(lat_u)) * cos(radians(lat_v)) * sin(dlon/2)**2
            length = R * 2 * atan2(sqrt(a), sqrt(1-a))

            edge_data = {'length': length, 'highway': highway, 'osmid': way['id']}
            G.add_edge(u, v, **edge_data)
            # Walking is bidirectional
            oneway = tags.get('oneway', 'no')
            if oneway not in ('yes', 'true', '1', '-1'):
                G.add_edge(v, u, **edge_data)

    # Set CRS attribute for OSMnx compatibility
    G.graph['crs'] = 'EPSG:4326'

    return G


def main():
    parser = argparse.ArgumentParser(description='Fetch walking network')
    add_city_argument(parser)
    parser.add_argument('--download-only', action='store_true',
                        help='Only download/cache the tiles; build the graph in a later run')
    parser.add_argument('--reuse-from', nargs='+', default=None,
                        help='tile_cache folder(s) of an earlier run with another bbox: all their '
                             'tiles are merged in and only the parts outside its bbox are '
                             'downloaded. If that run itself reused an older cache, pass that '
                             'folder too (its cache then only holds strips)')
    parser.add_argument('--reuse-bbox', default=None, help='bbox of the earlier run: south,west,north,east')
    parser.add_argument('--graphml', action='store_true',
                        help='Also save GraphML (slow and memory-hungry; 03 only needs the pickle)')
    args = parser.parse_args()

    city = get_city(args.city)
    network_dir = city['network_dir']
    network_dir.mkdir(parents=True, exist_ok=True)

    network_file = network_dir / "walking_network.graphml"
    network_cache = network_dir / "walking_network.pkl"

    print("=" * 60)
    print(f"Walking Network Fetcher — {city['name']}")
    print("=" * 60)
    print(f"Output: {network_dir}\n")

    if network_file.exists() or network_cache.exists():
        print(f"Network already exists at {network_cache if network_cache.exists() else network_file}")
        print("Delete it manually and re-run if you want to refresh.")
        return

    bbox = city['bbox']
    print(f"Bbox: {bbox['south']:.4f}-{bbox['north']:.4f} N, {bbox['west']:.4f}-{bbox['east']:.4f} E")

    cache_dir = network_dir / "tile_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    tiles = create_tiles(bbox, n=city.get('network_tiles', 4))  # big/dense cities: more tiles
    print(f"Downloading walking network in {len(tiles)} tiles...\n")

    start_time = datetime.now()
    jsons = []

    if args.reuse_from:
        # Old run: its tiles (and those of the run it reused, if any) cover the old bbox
        # completely (ways + all their nodes). Merge them all (a cache built with reuse holds
        # strips, not a plain grid); download only the new tiles' parts outside the old bbox.
        s_, w_, n_, e_ = (float(v) for v in args.reuse_bbox.split(','))
        ob = {'south': s_, 'west': w_, 'north': n_, 'east': e_}
        for folder in args.reuse_from:
            files = sorted(Path(folder).glob('tile_*.json'))
            if not files:
                sys.exit(f"ERROR: no tile_*.json in {folder}")
            jsons.extend(json.loads(f.read_text(encoding='utf-8')) for f in files)
        new_tiles = []
        for t in tiles:
            strips = split_against(t, ob)
            new_tiles.extend([t] if strips is None else strips)
        print(f"Reusing {len(jsons)} tiles from {', '.join(args.reuse_from)}; "
              f"{len(new_tiles)} tiles/strips to download instead of {len(tiles)} tiles")
        print()
        tiles = new_tiles

    # Every tile must be present: a missing tile would leave a hole in the network.
    # Successful tiles are cached, so failed ones are retried in later rounds (and
    # on a rerun) without downloading everything again.
    pending = list(tiles)
    for round_no in range(RETRY_ROUNDS + 1):
        if round_no > 0:
            wait = 120 * round_no
            print(f"\n  Retry round {round_no}: {len(pending)} failed tile(s), waiting {wait}s...")
            time.sleep(wait)
        failed = []
        for i, tile in enumerate(pending, 1):
            print(f"  [{i}/{len(pending)}] {tile['id']}...", end=" ", flush=True)
            cached = (cache_dir / f"tile_{tile['id']}.json").exists()
            data = fetch_tile_json(tile, cache_dir)
            if data is not None:
                n_elems = len(data.get('elements', []))
                print(f"{n_elems:,} elements")
                jsons.append(data)
            else:
                print("FAILED")
                failed.append(tile)

            if i < len(pending) and not cached:
                overpass_polite.pause(10)  # be polite between real downloads
        pending = failed
        if not pending:
            break

    if pending:
        ids = ', '.join(t['id'] for t in pending)
        print(f"\nERROR: {len(pending)} tile(s) still failing ({ids}). Network NOT built.")
        print("Rerun later: cached tiles are reused, only the missing ones are downloaded.")
        sys.exit(1)

    if args.download_only:
        print(f"\nAll {len(tiles)} tiles cached in {cache_dir}. Rerun without --download-only to build.")
        return

    print(f"\nBuilding graph from {len(jsons)} tiles...", end=" ", flush=True)
    G = build_graph_from_jsons(jsons)
    del jsons  # raw Overpass data (GBs for a large city) is no longer needed
    import gc; gc.collect()
    # Remove isolated nodes
    isolates = list(nx.isolates(G))
    G.remove_nodes_from(isolates)
    print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"Downloaded in {elapsed:.1f}s")

    # Pickle first: it's what 03 reads, and it's compact and fast to write
    print("Saving pickle...", end=' ', flush=True)
    with open(network_cache, 'wb') as f:
        pickle.dump(G, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Done ({network_cache.stat().st_size / 1024 / 1024:.1f} MB)")

    if args.graphml:  # optional: large text format, memory-hungry for big cities
        print("Saving GraphML...", end=' ', flush=True)
        ox.save_graphml(G, network_file)
        print(f"Done ({network_file.stat().st_size / 1024 / 1024:.1f} MB)")

    print("\n" + "=" * 60)
    print("Done! Network ready for isochrone generation.")
    print("=" * 60)


if __name__ == "__main__":
    main()
