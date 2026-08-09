"""
Fetch Walking Network for a city.
Downloads the OSM walking network via direct Overpass queries (tiled),
then builds a NetworkX graph with OSMnx.

Usage:
    python 02_fetch_walking_network.py --city warsaw
    python 02_fetch_walking_network.py --city gdansk
"""
import argparse
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

OVERPASS_URL = "https://overpass-api.de/api/interpreter"
HEADERS = {'User-Agent': 'QGIS-walking-network/1.0', 'Accept': '*/*'}

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


def fetch_tile_json(tile, cache_dir):
    """Fetch walking network JSON for a single tile, with caching and retries."""
    cache_file = cache_dir / f"tile_{tile['id']}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding='utf-8'))

    bbox_str = f"{tile['south']},{tile['west']},{tile['north']},{tile['east']}"
    query = WALK_QUERY.format(bbox=bbox_str)

    for attempt in range(3):
        try:
            if attempt > 0:
                wait = 60 * attempt
                print(f"retry {attempt} (wait {wait}s)...", end=" ", flush=True)
                time.sleep(wait)
            resp = requests.post(OVERPASS_URL, data={'data': query},
                                 headers=HEADERS, timeout=300)
            if resp.status_code == 429:
                wait = 60 * (2 ** attempt)
                print(f"rate-limited, wait {wait}s...", end=" ", flush=True)
                time.sleep(wait)
                continue
            if resp.status_code != 200:
                print(f"HTTP {resp.status_code}...", end=" ", flush=True)
                time.sleep(30)
                continue
            data = resp.json()
            cache_file.write_text(json.dumps(data), encoding='utf-8')
            return data
        except requests.exceptions.Timeout:
            print(f"timeout...", end=" ", flush=True)
        except Exception as e:
            print(f"error: {str(e)[:60]}...", end=" ", flush=True)
    return None


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

    cache_dir = network_dir / "tile_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)

    tiles = create_tiles(bbox, n=4)
    print(f"Downloading walking network in {len(tiles)} tiles...\n")

    start_time = datetime.now()
    jsons = []

    for i, tile in enumerate(tiles, 1):
        print(f"  [{i}/{len(tiles)}] {tile['id']}...", end=" ", flush=True)
        data = fetch_tile_json(tile, cache_dir)
        if data is not None:
            n_elems = len(data.get('elements', []))
            print(f"{n_elems:,} elements")
            jsons.append(data)
        else:
            print("FAILED")

        if i < len(tiles):
            time.sleep(10)

    if not jsons:
        print("No tiles downloaded!")
        return

    print(f"\nBuilding graph from {len(jsons)} tiles...", end=" ", flush=True)
    G = build_graph_from_jsons(jsons)
    # Remove isolated nodes
    isolates = list(nx.isolates(G))
    G.remove_nodes_from(isolates)
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
