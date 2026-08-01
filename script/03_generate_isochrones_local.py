"""
Generate Walking Isochrones for Transit Stops - Local Version
Uses OSMnx walking network + ego_graph to build 5-minute walking polygons.
No API keys or rate limits.

Usage:
    python 03_generate_isochrones_local.py <operator>

    operator: ztm, km, wkd, metro (or 'all' to process everything)

Each isochrone carries route_ids from its stop so that overlapping isochrones
can be deduplicated by transit line in downstream processing.
"""
import sys
import pickle
import geopandas as gpd
import pandas as pd
import osmnx as ox
import networkx as nx
from pathlib import Path
from datetime import datetime
from shapely.geometry import Point, LineString
from shapely.ops import unary_union

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
NETWORK_DIR = PROJECT_DIR / "network"
DATA_DIR = PROJECT_DIR / "_data" / "2024_03_27"

NETWORK_FILE  = NETWORK_DIR / "warsaw_walking_network.graphml"
NETWORK_CACHE = NETWORK_DIR / "warsaw_walking_network.pkl"

WALKING_SPEED = 4.5   # km/h
TIME_LIMIT = 5        # minutes
DISTANCE_M = TIME_LIMIT * (WALKING_SPEED * 1000 / 60)  # 375 m

# Operator configs: input CSV path, output GPKG path
OPERATORS = {
    'ztm': {
        'input': DATA_DIR / "ZTM" / "stops_trip_count_ZTM.csv",
        'output': DATA_DIR / "ZTM" / "isochrones" / "ZTM_isochrones.gpkg",
    },
    'km': {
        'input': DATA_DIR / "KM" / "stops_trip_count_KM.csv",
        'output': DATA_DIR / "KM" / "isochrones" / "KM_isochrones.gpkg",
    },
    'wkd': {
        'input': DATA_DIR / "WKD" / "stops_trip_count_wkd.csv",
        'output': DATA_DIR / "WKD" / "isochrones" / "WKD_isochrones.gpkg",
    },
    'metro': {
        'input': DATA_DIR / "metro" / "stops_trip_count_metro.csv",
        'output': DATA_DIR / "metro" / "isochrones" / "metro_isochrones.gpkg",
    },
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
        G = ox.load_graphml(NETWORK_FILE)
        print(f"{len(G.nodes):,} nodes, {len(G.edges):,} edges")
        return G

    print("Network not found! Run 02_fetch_walking_network.py first.")
    return None


def create_isochrone(G, point, distance_m):
    """
    Create isochrone polygon by buffering reachable nodes and edges.

    Args:
        G: NetworkX walking graph
        point: Shapely Point in WGS84
        distance_m: Maximum walking distance in meters

    Returns:
        Shapely polygon (WGS84) or None
    """
    nearest_node = ox.distance.nearest_nodes(G, point.x, point.y)

    # Skip if nearest node is too far (stop outside network)
    node_data = G.nodes[nearest_node]
    node_pt = Point(node_data['x'], node_data['y'])
    poi_metric = gpd.GeoSeries([point], crs="EPSG:4326").to_crs("EPSG:2180").iloc[0]
    node_metric = gpd.GeoSeries([node_pt], crs="EPSG:4326").to_crs("EPSG:2180").iloc[0]
    if poi_metric.distance(node_metric) > 500:
        return None

    subgraph = nx.ego_graph(G, nearest_node, radius=distance_m, distance='length')
    if len(subgraph.nodes) < 3:
        return None

    # Buffer nodes (intersections)
    node_points = [Point(G.nodes[n]['x'], G.nodes[n]['y']) for n in subgraph.nodes()]
    nodes_metric = gpd.GeoSeries(node_points, crs="EPSG:4326").to_crs("EPSG:2180")
    buffered_nodes = nodes_metric.buffer(50)

    # Buffer edges (street segments) to fill gaps on long straight roads
    edge_lines = []
    for u, v in subgraph.edges():
        u_pt = Point(G.nodes[u]['x'], G.nodes[u]['y'])
        v_pt = Point(G.nodes[v]['x'], G.nodes[v]['y'])
        edge_lines.append(LineString([u_pt, v_pt]))

    if edge_lines:
        edges_metric = gpd.GeoSeries(edge_lines, crs="EPSG:4326").to_crs("EPSG:2180")
        buffered_edges = edges_metric.buffer(50)
        all_buffers = list(buffered_nodes) + list(buffered_edges)
        isochrone_metric = unary_union(all_buffers)
    else:
        isochrone_metric = unary_union(buffered_nodes.tolist())

    isochrone_wgs84 = (
        gpd.GeoSeries([isochrone_metric], crs="EPSG:2180")
        .to_crs("EPSG:4326")
        .iloc[0]
    )
    return isochrone_wgs84


def process_operator(operator_id, G):
    """Generate isochrones for a single operator."""
    config = OPERATORS[operator_id]
    input_file = config['input']
    output_file = config['output']

    print(f"\n{'='*60}")
    print(f"  {operator_id.upper()} Isochrone Generator")
    print(f"{'='*60}")
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}")
    print(f"Walking: {TIME_LIMIT} min / {DISTANCE_M:.0f} m at {WALKING_SPEED} km/h\n")

    if not input_file.exists():
        print(f"Input not found: {input_file}")
        print("Run the corresponding 01_*.py script first.")
        return

    # Load stops
    stops = pd.read_csv(input_file, dtype={'stop_id': str, 'route_ids': str})
    print(f"Loaded {len(stops)} stops")

    output_file.parent.mkdir(parents=True, exist_ok=True)

    start_time = datetime.now()
    results = []
    skipped = 0

    for seq, (_, stop) in enumerate(stops.iterrows()):
        if seq % 100 == 0 and seq > 0:
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = elapsed / seq
            remaining = rate * (len(stops) - seq)
            print(f"  [{seq}/{len(stops)}] {elapsed:.0f}s elapsed, ~{remaining:.0f}s remaining")

        point = Point(stop['stop_lon'], stop['stop_lat'])
        polygon = create_isochrone(G, point, DISTANCE_M)

        if polygon is not None and not polygon.is_empty:
            record = {
                'stop_id': stop.get('stop_id', seq),
                'trip_count': int(stop.get('trip_count', 0)),
                'unique_routes': int(stop.get('unique_routes', 0)),
                'route_ids': stop.get('route_ids', ''),
                'time_minutes': TIME_LIMIT,
                'distance_m': DISTANCE_M,
                'geometry': polygon,
            }
            results.append(record)
        else:
            skipped += 1

    print(f"\nGenerated: {len(results)}/{len(stops)} (skipped {skipped})")

    if not results:
        print("No isochrones generated!")
        return

    gdf = gpd.GeoDataFrame(results, crs="EPSG:4326")
    gdf_metric = gdf.to_crs("EPSG:2180")
    gdf['area_ha'] = gdf_metric.geometry.area / 10000

    gdf.to_file(output_file, driver="GPKG")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nDone in {elapsed/60:.1f} min")
    print(f"  Isochrones: {len(gdf)}")
    print(f"  Avg area: {gdf['area_ha'].mean():.1f} ha")
    print(f"  Saved to: {output_file}")


def main():
    if len(sys.argv) < 2:
        print("Usage: python 03_generate_isochrones_local.py <operator>")
        print(f"  Operators: {', '.join(OPERATORS.keys())}, all")
        sys.exit(1)

    target = sys.argv[1].lower()

    if target == 'all':
        operators = list(OPERATORS.keys())
    elif target in OPERATORS:
        operators = [target]
    else:
        print(f"Unknown operator: {target}")
        print(f"Choose from: {', '.join(OPERATORS.keys())}, all")
        sys.exit(1)

    G = load_network()
    if G is None:
        return

    for op in operators:
        process_operator(op, G)

    print(f"\n{'='*60}")
    print("All done!")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
