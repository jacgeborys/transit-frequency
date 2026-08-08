"""
Generate Walking Isochrones for Transit Stops - Local Version
Uses OSMnx walking network + ego_graph to build 5-minute walking polygons.
No API keys or rate limits.

Usage:
    python 03_generate_isochrones_local.py --city warsaw [--sample N] [data_folder]
    python 03_generate_isochrones_local.py --city poznan

    --sample N   Process only the first N stops (for testing)
    data_folder  Explicit path (default: most recent in _data/<city>/)

Reads stops_trip_count.csv (output of 01_calculate_trip_counts.py).
Each isochrone carries route_ids from its stop so that overlapping isochrones
can be deduplicated by transit line in downstream processing.
"""
import sys
import argparse
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

from cities import get_city, add_city_argument

WALKING_SPEED = 4.5   # km/h
TIME_LIMIT = 5        # minutes
DISTANCE_M = TIME_LIMIT * (WALKING_SPEED * 1000 / 60)  # 375 m
BUFFER_M = 50         # buffer around nodes and edges


def load_network(city: dict):
    """Load pre-downloaded walking network. Try pickle first, fall back to GraphML."""
    network_dir = city['network_dir']
    cache_file = network_dir / "walking_network.pkl"
    graphml_file = network_dir / "walking_network.graphml"

    if cache_file.exists():
        print("Loading network from cache...", end=' ', flush=True)
        with open(cache_file, 'rb') as f:
            G = pickle.load(f)
        print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")
        return G

    if graphml_file.exists():
        print("Loading network from GraphML...", end=' ', flush=True)
        import osmnx as ox
        G = ox.load_graphml(graphml_file)
        print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")
        return G

    print(f"Network not found in {network_dir}!")
    print("Run 02_fetch_walking_network.py first.")
    return None


def build_spatial_index(G, crs_metric: str):
    """
    Build a KDTree for fast nearest-node lookups and pre-project coords.
    Returns: (node_ids array, KDTree on lon/lat, dict node_id->(x_m, y_m))
    """
    print("Building spatial index + projecting coords...", end=' ', flush=True)
    to_metric = Transformer.from_crs("EPSG:4326", crs_metric, always_xy=True)

    node_ids = []
    lonlats = []
    node_coords_metric = {}

    for n, data in G.nodes(data=True):
        node_ids.append(n)
        lonlats.append((data['x'], data['y']))
        mx, my = to_metric.transform(data['x'], data['y'])
        node_coords_metric[n] = (mx, my)

    node_ids = np.array(node_ids)
    lonlats = np.array(lonlats)
    tree = cKDTree(lonlats)

    print(f"Done ({len(node_ids):,} nodes)")
    return node_ids, tree, node_coords_metric


def find_nearest_node(tree, node_ids, lon, lat):
    """Find nearest graph node using KDTree."""
    _, idx = tree.query([lon, lat])
    return node_ids[idx]


def create_isochrone(G, point_metric, distance_m, node_coords, nearest_node):
    """Create isochrone polygon using pre-projected coordinates."""
    nx_m, ny_m = node_coords[nearest_node]
    dist = ((point_metric[0] - nx_m)**2 + (point_metric[1] - ny_m)**2) ** 0.5
    if dist > 500:
        return None

    subgraph = nx.ego_graph(G, nearest_node, radius=distance_m, distance='length')
    if len(subgraph.nodes) < 3:
        return None

    node_buffers = [Point(node_coords[n]).buffer(BUFFER_M) for n in subgraph.nodes()]
    edge_buffers = [
        LineString([node_coords[u], node_coords[v]]).buffer(BUFFER_M)
        for u, v in subgraph.edges()
    ]

    return unary_union(node_buffers + edge_buffers)


def find_latest_data_dir(city: dict) -> Path:
    """Find the most recent data folder for a city."""
    base = city['data_dir']
    if not base.exists():
        raise FileNotFoundError(f"No data directory for {city['name']}: {base}")
    data_dirs = sorted(
        [d for d in base.iterdir() if d.is_dir() and (d / 'stops_trip_count.csv').exists()],
        key=lambda x: x.name, reverse=True
    )
    if not data_dirs:
        raise FileNotFoundError(
            f"No stops_trip_count.csv in {base}. Run 01_calculate_trip_counts.py first."
        )
    return data_dirs[0]


def main():
    parser = argparse.ArgumentParser(description='Generate walking isochrones')
    add_city_argument(parser)
    parser.add_argument('--sample', type=int, default=None, help='Process only N stops (testing)')
    parser.add_argument('data_folder', nargs='?', help='Data folder (default: most recent)')
    args = parser.parse_args()

    city = get_city(args.city)
    crs_metric = city['crs_metric']

    data_dir = Path(args.data_folder) if args.data_folder else find_latest_data_dir(city)
    input_file = data_dir / "stops_trip_count.csv"
    output_file = data_dir / ("isochrones_sample.gpkg" if args.sample else "isochrones.gpkg")

    print("=" * 60)
    print(f"Isochrone Generator — {city['name']}")
    print("=" * 60)
    print(f"Input:   {input_file}")
    print(f"Output:  {output_file}")
    print(f"CRS:     {crs_metric}")
    print(f"Walking: {TIME_LIMIT} min / {DISTANCE_M:.0f} m at {WALKING_SPEED} km/h\n")

    if not input_file.exists():
        print(f"Input not found: {input_file}")
        print("Run 01_calculate_trip_counts.py first.")
        return

    stops = pd.read_csv(input_file, dtype={'stop_id': str, 'route_ids': str})
    if args.sample:
        stops = stops.head(args.sample)
        print(f"SAMPLE MODE: processing {len(stops)} stops")
    else:
        print(f"Loaded {len(stops)} stops")

    G = load_network(city)
    if G is None:
        return

    node_ids, tree, node_coords = build_spatial_index(G, crs_metric)
    to_metric = Transformer.from_crs("EPSG:4326", crs_metric, always_xy=True)

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
                'route_trip_counts': stop.get('route_trip_counts', ''),
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
    gdf = gpd.GeoDataFrame(results, crs=crs_metric)
    gdf['area_ha'] = gdf.geometry.area / 10000
    gdf = gdf.to_crs("EPSG:4326")
    gdf.to_file(output_file, driver="GPKG")

    print("Done")
    print(f"\n  Avg area: {gdf['area_ha'].mean():.1f} ha")
    print(f"  Saved to: {output_file}")


if __name__ == "__main__":
    main()
