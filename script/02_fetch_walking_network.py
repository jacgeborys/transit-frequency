"""
Fetch Walking Network for Warsaw
Downloads and caches the full OSM walking network for the Warsaw bounding box.
Uses OSMnx — no API keys needed.

If the zabka project already has a cached network, this script will copy it
instead of re-downloading.
"""
import osmnx as ox
from pathlib import Path
from datetime import datetime
import pickle
import shutil
from shapely.geometry import box

SCRIPT_DIR = Path(__file__).resolve().parent
NETWORK_DIR = SCRIPT_DIR.parent / "network"
NETWORK_DIR.mkdir(parents=True, exist_ok=True)

NETWORK_FILE  = NETWORK_DIR / "warsaw_walking_network.graphml"
NETWORK_CACHE = NETWORK_DIR / "warsaw_walking_network.pkl"

# Warsaw bounding box
WARSAW_BBOX = {
    'south': 52.0977,
    'west':  20.8519,
    'north': 52.3690,
    'east':  21.2711,
}

# Zabka project network (reuse if available)
ZABKA_NETWORK_DIR = Path(r"D:\QGIS\mapy_warszawy_misc\zabka\network")

MAX_QUERY_AREA = 50_000_000  # 50 km²


def main():
    print("=" * 60)
    print("Walking Network Fetcher for Warsaw")
    print("=" * 60)
    print(f"Output: {NETWORK_DIR}\n")

    if NETWORK_FILE.exists():
        print(f"Network already exists at {NETWORK_FILE}")
        print("Delete it manually and re-run if you want to refresh.")
        return

    # Try to reuse the zabka project's cached network
    zabka_pkl = ZABKA_NETWORK_DIR / "warsaw_walking_network.pkl"
    zabka_graphml = ZABKA_NETWORK_DIR / "warsaw_walking_network.graphml"

    if zabka_pkl.exists() and zabka_graphml.exists():
        print("Found existing network in zabka project — copying...")
        shutil.copy2(zabka_graphml, NETWORK_FILE)
        shutil.copy2(zabka_pkl, NETWORK_CACHE)
        print(f"  GraphML: {NETWORK_FILE.stat().st_size / 1024 / 1024:.1f} MB")
        print(f"  Pickle:  {NETWORK_CACHE.stat().st_size / 1024 / 1024:.1f} MB")
        print("Done!")
        return

    # Download fresh from OSM
    print("Downloading walking network from OpenStreetMap...\n")

    ox.settings.log_console = True
    ox.settings.max_query_area_size = MAX_QUERY_AREA
    ox.settings.requests_pause = 2

    bbox_polygon = box(
        WARSAW_BBOX['west'],
        WARSAW_BBOX['south'],
        WARSAW_BBOX['east'],
        WARSAW_BBOX['north'],
    )

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
    ox.save_graphml(G, NETWORK_FILE)
    print(f"Done ({NETWORK_FILE.stat().st_size / 1024 / 1024:.1f} MB)")

    print("Saving pickle...", end=' ', flush=True)
    with open(NETWORK_CACHE, 'wb') as f:
        pickle.dump(G, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"Done ({NETWORK_CACHE.stat().st_size / 1024 / 1024:.1f} MB)")

    print("\n" + "=" * 60)
    print("Done! Network ready for isochrone generation.")
    print("=" * 60)


if __name__ == "__main__":
    main()
