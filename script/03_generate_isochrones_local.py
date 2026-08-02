"""
Generate Walking Isochrones for Transit Stops - Local Version
Uses OSMnx walking network + ego_graph to build 5-minute walking polygons.
No API keys or rate limits.

Usage:
    python 03_generate_isochrones_local.py [--sample N] [data_folder]

    --sample N   Process only the first N stops (for testing)
    Defaults: _data/2026_08_02

Reads stops_trip_count.csv (output of 01_calculate_trip_counts.py).
Each isochrone carries route_ids from its stop so that overlapping isochrones
can be deduplicated by transit line in downstream processing.
"""
import sys
import pickle
import geopandas as gpd
import pandas as pd
import numpy as np
import networkx as nx
from pathlib import Path
from datetime import datetime
from shapely.geometry import Point, LineString
from shapely.ops import unary_union
from pyproj import Transformer
from scipy.spatial import cKDTree

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
NETWORK_DIR = PROJECT_DIR / "network"

NETWORK_FILE  = NETWORK_DIR / "warsaw_walking_network.graphml"
NETWORK_CACHE = NETWORK_DIR / "warsaw_walking_network.pkl"

WALKING_SPEED = 4.5   # km/h
TIME_LIMIT = 5        # minutes
DISTANCE_M = TIME_LIMIT * (WALKING_SPEED * 1000 / 60)  # 375 m
BUFFER_M = 50         # buffer around nodes and edges

# Warsaw bounding box (matches the walking network extent)
WARSAW_BBOX = {
    'south': 52.0977, 'north': 52.3690,
    'west': 20.8519, 'east': 21.2711,
}


def load_network():
    """Load pre-downloaded walking network. Try pickle first, fall back to GraphML."""
    if NETWORK_CACHE.exists():
        print("Loading network from cache...", end=' ', flush=True)
        with open(NETWORK_CACHE, 'rb') as f:
            G = pickle.load(f)
        print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")
        return G

    if NETWORK_FILE.exists():
        print("Loading network from GraphML...", end=' ', flush=True)
        import osmnx as ox
        G = ox.load_graphml(NETWORK_FILE)
        print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")
        return G

    print("Network not found! Run 02_fetch_walking_network.py first.")
    return None


def build_spatial_index(G):
    """
    Build a KDTree for fast nearest-node lookups and pre-project coords to EPSG:2180.
    Returns: (node_ids array, KDTree on lon/lat, dict node_id->(x_m, y_m))
    """
    print("Building spatial index + projecting coords...", end=' ', flush=True)
    to_metric = Transformer.from_crs("EPSG:4326", "EPSG:2180", always_xy=True)

    node_ids = []
    lonlats = []
    node_coords_metric = {}

    for n, data in G.nodes(data=True):
        node_ids.append(n)
        lonlats.append((data['x'], data['y']))  # lon, lat
        mx, my = to_metric.transform(data['x'], data['y'])  # lon, lat -> x, y
        node_coords_metric[n] = (mx, my)

    node_ids = np.array(node_ids)
    lonlats = np.array(lonlats)
    tree = cKDTree(lonlats)

    print(f"Done ({len(node_ids):,} nodes)")
    return node_ids, tree, node_coords_metric


def find_nearest_node(tree, node_ids, lon, lat):
    """Find nearest graph node using KDTree (near-instant)."""
    _, idx = tree.query([lon, lat])
    return node_ids[idx]


def create_isochrone(G, point_metric, distance_m, node_coords, nearest_node):
    """
    Create isochrone polygon using pre-projected coordinates.
    All geometry work done in EPSG:2180 (metric).
    """
    nx_m, ny_m = node_coords[nearest_node]
    dist = ((point_metric[0] - nx_m)**2 + (point_metric[1] - ny_m)**2) ** 0.5
    if dist > 500:
        return None

    subgraph = nx.ego_graph(G, nearest_node, radius=distance_m, distance='length')
    if len(subgraph.nodes) < 3:
        return None

    # Buffer nodes + edges in metric coords
    node_buffers = [Point(node_coords[n]).buffer(BUFFER_M) for n in subgraph.nodes()]
    edge_buffers = [
        LineString([node_coords[u], node_coords[v]]).buffer(BUFFER_M)
        for u, v in subgraph.edges()
    ]

    return unary_union(node_buffers + edge_buffers)


def main():
    default_data = PROJECT_DIR / "_data" / "2026_08_02"

    # Parse args
    args = sys.argv[1:]
    sample_n = None
    if '--sample' in args:
        idx = args.index('--sample')
        sample_n = int(args[idx + 1])
        args = args[:idx] + args[idx+2:]

    data_dir = Path(args[0]) if args else default_data
    input_file = data_dir / "stops_trip_count.csv"
    output_file = data_dir / ("isochrones_sample.gpkg" if sample_n else "isochrones.gpkg")

    print("=" * 60)
    print("Isochrone Generator (Local - OSMnx)")
    print("=" * 60)
    print(f"Input:   {input_file}")
    print(f"Output:  {output_file}")
    print(f"Walking: {TIME_LIMIT} min / {DISTANCE_M:.0f} m at {WALKING_SPEED} km/h\n")

    if not input_file.exists():
        print(f"Input not found: {input_file}")
        print("Run 01_calculate_trip_counts.py first.")
        return

    stops = pd.read_csv(input_file, dtype={'stop_id': str, 'route_ids': str})
    if sample_n:
        stops = stops.head(sample_n)
        print(f"SAMPLE MODE: processing {len(stops)} stops")
    else:
        print(f"Loaded {len(stops)} stops")

    G = load_network()
    if G is None:
        return

    # Build spatial index + pre-project coordinates (replaces slow ox.nearest_nodes)
    node_ids, tree, node_coords = build_spatial_index(G)
    to_metric = Transformer.from_crs("EPSG:4326", "EPSG:2180", always_xy=True)

    print(f"\nGenerating isochrones...\n")
    start_time = datetime.now()
    results = []
    skipped = 0

    for seq, (_, stop) in enumerate(stops.iterrows()):
        if seq % 500 == 0 and seq > 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = elapsed / seq
            remaining = rate * (len(stops) - seq)
            print(f"  [{seq}/{len(stops)}] {elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining")

        sx, sy = to_metric.transform(stop['stop_lon'], stop['stop_lat'])
        nearest_node = find_nearest_node(tree, node_ids, stop['stop_lon'], stop['stop_lat'])

        polygon_metric = create_isochrone(G, (sx, sy), DISTANCE_M, node_coords, nearest_node)

        if polygon_metric is not None and not polygon_metric.is_empty:
            results.append({
                'stop_id': stop['stop_id'],
                'stop_name': stop.get('stop_name', ''),
                'trip_count': int(stop.get('trip_count', 0)),
                'unique_routes': int(stop.get('unique_routes', 0)),
                'route_ids': stop.get('route_ids', ''),
                'bus': int(stop.get('bus', 0)),
                'tram': int(stop.get('tram', 0)),
                'train': int(stop.get('train', 0)),
                'metro': int(stop.get('metro', 0)),
                'time_minutes': TIME_LIMIT,
                'distance_m': DISTANCE_M,
                'geometry': polygon_metric,
            })
        else:
            skipped += 1

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n{'='*60}")
    print(f"Generated: {len(results)}/{len(stops)} isochrones (skipped {skipped})")
    print(f"Time: {elapsed/60:.1f} min")
    print(f"{'='*60}")

    if not results:
        print("No isochrones generated!")
        return

    print("Saving...", end=' ', flush=True)
    gdf = gpd.GeoDataFrame(results, crs="EPSG:2180")
    gdf['area_ha'] = gdf.geometry.area / 10000
    gdf = gdf.to_crs("EPSG:4326")
    gdf.to_file(output_file, driver="GPKG")

    print("Done")
    print(f"\n  Avg area: {gdf['area_ha'].mean():.1f} ha")
    print(f"  Saved to: {output_file}")


if __name__ == "__main__":
    main()
