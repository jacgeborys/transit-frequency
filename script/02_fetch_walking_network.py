"""
Fetch Walking Network for a city.
Downloads and caches the full OSM walking network for the city's bounding box.
Uses OSMnx -- no API keys needed.

Usage:
    python 02_fetch_walking_network.py --city warsaw
    python 02_fetch_walking_network.py --city poznan
"""
import argparse
import osmnx as ox
import pickle
from pathlib import Path
from datetime import datetime
from shapely.geometry import box

from cities import get_city, add_city_argument

MAX_QUERY_AREA = 50_000_000  # 50 km^2


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
    print("Downloading walking network from OpenStreetMap...\n")

    ox.settings.log_console = True
    ox.settings.max_query_area_size = MAX_QUERY_AREA
    ox.settings.requests_pause = 2

    bbox_polygon = box(bbox['west'], bbox['south'], bbox['east'], bbox['north'])
    start_time = datetime.now()

    G = ox.graph_from_polygon(
        bbox_polygon,
        network_type='walk',
        simplify=True,
        retain_all=False,
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nNetwork downloaded in {elapsed:.1f}s")
    print(f"  Nodes: {len(G.nodes):,}")
    print(f"  Edges: {len(G.edges):,}")

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
