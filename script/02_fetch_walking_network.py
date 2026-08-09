"""
Fetch Walking Network for a city.
Downloads and caches the full OSM walking network for the city's bounding box.
Uses OSMnx -- no API keys needed. Large areas are split into tiles and merged.

Usage:
    python 02_fetch_walking_network.py --city warsaw
    python 02_fetch_walking_network.py --city poznan
"""
import argparse
import warnings
import osmnx as ox
import networkx as nx
import pickle
import time as time_mod
from pathlib import Path
from datetime import datetime
from shapely.geometry import box

from cities import get_city, add_city_argument


def create_tiles(bbox, n=3):
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
            })
    return tiles


def fetch_tile(tile, attempt_waits=(0, 60, 120)):
    """Fetch a single tile with retries and increasing waits."""
    polygon = box(tile['west'], tile['south'], tile['east'], tile['north'])
    for i, wait in enumerate(attempt_waits):
        if wait > 0:
            print(f"retry {i} (wait {wait}s)...", end=" ", flush=True)
            time_mod.sleep(wait)
        try:
            G = ox.graph_from_polygon(
                polygon,
                network_type='walk',
                simplify=True,
                retain_all=False,
            )
            return G
        except Exception as e:
            err = str(e)[:80]
            if i == len(attempt_waits) - 1:
                print(f"FAILED: {err}")
    return None


def main():
    parser = argparse.ArgumentParser(description='Fetch walking network')
    add_city_argument(parser)
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

    if network_file.exists():
        print(f"Network already exists at {network_file}")
        print("Delete it manually and re-run if you want to refresh.")
        return

    bbox = city['bbox']
    print(f"Bbox: {bbox['south']:.4f}-{bbox['north']:.4f} N, {bbox['west']:.4f}-{bbox['east']:.4f} E")

    warnings.filterwarnings('ignore', category=FutureWarning, module='osmnx')
    warnings.filterwarnings('ignore', category=UserWarning, module='osmnx')
    ox.settings.log_console = False
    ox.settings.http_user_agent = "QGIS-walking-network/1.0"
    ox.settings.requests_timeout = 300
    ox.settings.overpass_rate_limit = False

    # Patch OSMnx's session to use our User-Agent
    import requests as req_lib
    _orig_post = req_lib.Session.post
    def _patched_post(self, url, **kwargs):
        headers = kwargs.get('headers', {}) or {}
        headers.setdefault('User-Agent', 'QGIS-walking-network/1.0')
        kwargs['headers'] = headers
        return _orig_post(self, url, **kwargs)
    req_lib.Session.post = _patched_post

    tiles = create_tiles(bbox, n=4)
    print(f"Downloading walking network in {len(tiles)} tiles...\n")

    start_time = datetime.now()
    graphs = []

    for i, tile in enumerate(tiles, 1):
        print(f"  [{i}/{len(tiles)}]", end=" ", flush=True)

        G = fetch_tile(tile)
        if G is not None:
            print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")
            graphs.append(G)

        if i < len(tiles):
            time_mod.sleep(15)

    # Restore original
    req_lib.Session.post = _orig_post

    if not graphs:
        print("No tiles downloaded!")
        return

    print(f"\nMerging {len(graphs)} graphs...", end=" ", flush=True)
    G = graphs[0]
    for g in graphs[1:]:
        G = nx.compose(G, g)
    print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"Downloaded in {elapsed:.1f}s")

    print("Saving GraphML...", end=' ', flush=True)
    ox.save_graphml(G, network_file)
    print(f"Done ({network_file.stat().st_size / 1024 / 1024:.1f} MB)")

    print("Saving pickle...", end=' ', flush=True)
    with open(network_cache, 'wb') as f:
        pickle.dump(G, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Done ({network_cache.stat().st_size / 1024 / 1024:.1f} MB)")

    print("\n" + "=" * 60)
    print("Done! Network ready for isochrone generation.")
    print("=" * 60)


if __name__ == "__main__":
    main()
