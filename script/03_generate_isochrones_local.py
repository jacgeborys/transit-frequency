"""
Generate Walking Isochrones for Transit Stops - Local Version
Uses OSMnx walking network + scipy sparse graph to build 5-minute walking polygons.
No API keys or rate limits.

Usage:
    python 03_generate_isochrones_local.py --city warsaw [--sample N] [--barriers] [data_folder]
    python 03_generate_isochrones_local.py --city poznan

    --sample N   Process only the first N stops (for testing)
    --barriers   Off-network spread respects fences/walls (impassable) and
                 buildings (enterable up to 10 m, never crossed).
                 Output: isochrones_barriers.gpkg
    data_folder  Explicit path (default: most recent in _data/<city>/)

Barrier mode needs <osm_dir>/buildings.gpkg and <osm_dir>/barriers.gpkg
(fetch with D:\QGIS\osm_basemap\fetch_osm_basemap.py --only barriers).
Network routing is identical in both modes; only the 50 m off-network
spread around reachable streets/paths changes: instead of a plain buffer it
is a raster cost-distance (2 m cells) that cannot pass through barriers.

Reads stops_trip_count.csv (output of 01_calculate_trip_counts.py).
Each isochrone carries route_ids from its stop so that overlapping isochrones
can be deduplicated by transit line in downstream processing.
"""
import sys
import argparse
import pickle
import shutil
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
from scipy.sparse import csr_matrix, identity, bmat
from scipy.sparse.csgraph import dijkstra
from scipy.ndimage import distance_transform_edt, binary_dilation, label

from cities import get_city, add_city_argument
from access_rules import closed_gate_ids, private_way_ids

WALKING_SPEED = 4.5   # km/h
TIME_LIMIT = 5        # minutes
DISTANCE_M = TIME_LIMIT * (WALKING_SPEED * 1000 / 60)  # 375 m
BUFFER_M = 50         # buffer around nodes and edges

# Barrier mode
CELL_M = 2.0                      # raster resolution
BUILDING_PERMEABILITY_M = 10      # how far into a building the isochrone reaches
EXCLUDED_BUILDING_TYPES = {'roof', 'carport'}  # open structures, walkable underneath
MIN_HOLE_M2 = 200                 # enclosed holes smaller than this are filled
MIN_PART_M2 = 50                  # detached fragments smaller than this are dropped
OPEN, BUILDING, BARRIER = 0, 1, 2
EIGHT = np.ones((3, 3), dtype=bool)

CHECKPOINT_EVERY = 250            # stops per checkpoint chunk (resume after a crash)
DEFAULT_WORKERS = 3               # RAM-friendly: workers memory-map the shared arrays


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


def convert_to_sparse(G, crs_metric: str, access=None):
    """
    Convert NetworkX graph to scipy sparse matrix + coordinate arrays.
    Returns: (node_ids, KDTree, coords_metric, sparse_matrix, node_to_idx, gate_net)
    gate_net is None unless access = {'closed_gates': set, 'private_ways': set}.
    The NetworkX graph can be freed after this to save memory.
    """
    print("Converting graph to sparse matrix...", end=' ', flush=True)
    to_metric = Transformer.from_crs("EPSG:4326", crs_metric, always_xy=True)

    nids = list(G.nodes())
    node_to_idx = {n: i for i, n in enumerate(nids)}
    n_nodes = len(nids)

    lons = [G.nodes[n]['x'] for n in nids]
    lats = [G.nodes[n]['y'] for n in nids]

    # Vectorized projection
    mx, my = to_metric.transform(lons, lats)
    coords_metric = np.column_stack([mx, my])

    # Build sparse adjacency matrix
    rows, cols, weights = [], [], []
    private = []
    for u, v, data in G.edges(data=True):
        ui, vi = node_to_idx[u], node_to_idx[v]
        length = data.get('length', 0)
        rows.append(ui)
        cols.append(vi)
        weights.append(length)
        if access is not None:
            osmid = data.get('osmid')
            way_ids = osmid if isinstance(osmid, list) else [osmid]
            private.append(u in access['closed_gates'] or v in access['closed_gates']
                           or any(_as_int(w) in access['private_ways'] for w in way_ids))

    sparse = csr_matrix((weights, (rows, cols)), shape=(n_nodes, n_nodes))

    node_ids = np.array(nids)
    lonlats = np.column_stack([lons, lats])
    tree = cKDTree(lonlats)

    print(f"Done ({n_nodes:,} nodes, {len(rows):,} edges)")
    if access is None:
        return node_ids, tree, coords_metric, sparse, node_to_idx, None

    # Public / private split for gate-aware routing
    rows, cols, weights, private = map(np.asarray, (rows, cols, weights, private))
    pub = ~private
    sparse_public = csr_matrix((weights[pub], (rows[pub], cols[pub])), shape=(n_nodes, n_nodes))
    sparse_private = csr_matrix((weights[private], (rows[private], cols[private])),
                                shape=(n_nodes, n_nodes))
    # Private nodes: no public edge touches them (estate interiors, closed gates)
    has_public = (np.diff(sparse_public.indptr) > 0) | (np.bincount(
        sparse_public.indices, minlength=n_nodes) > 0)
    # Two-layer graph: public layer [0, N), residents layer [N, 2N).
    # Public -> residents transitions everywhere (tiny cost), never back, and the
    # residents layer only has private edges: residents can leave their estate
    # to the public network, but nobody can cut through an estate.
    eps = identity(n_nodes, format='csr') * 1e-3
    two_layer = bmat([[sparse_public, eps], [None, sparse_private]], format='csr')
    print(f"  Access: {private.sum():,} of {len(private):,} edges private "
          f"({(~has_public).sum():,} private-only nodes)")
    gate_net = {
        'public': sparse_public,
        'full': (sparse_public + sparse_private).tocsr(),
        'two_layer': two_layer,
        'private_node': ~has_public,
        'public_tree': cKDTree(lonlats[has_public]),
        'public_idx': np.where(has_public)[0],
    }
    return node_ids, tree, coords_metric, sparse, node_to_idx, gate_net


def _as_int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def reachable_two_layer(source_idx, stop_metric, coords_metric, gate_net, distance_m):
    """
    Gate-aware reach: (public nodes, residents-only nodes), or None.
    Residents-only = private nodes reached only via private edges.
    """
    nx_m, ny_m = coords_metric[source_idx]
    if ((stop_metric[0] - nx_m)**2 + (stop_metric[1] - ny_m)**2) ** 0.5 > 500:
        return None
    n = len(coords_metric)
    dists = dijkstra(gate_net['two_layer'], directed=True, indices=source_idx, limit=distance_m)
    pub = np.isfinite(dists[:n])
    res = np.isfinite(dists[n:]) & gate_net['private_node'] & ~pub
    if pub.sum() < 3:
        return None
    return np.where(pub)[0], np.where(res)[0]


def reachable_nodes(source_idx, stop_metric, coords_metric, sparse, distance_m):
    """Indices of network nodes within distance_m of the source, or None."""
    # Check if stop is near any graph node
    nx_m, ny_m = coords_metric[source_idx]
    dist = ((stop_metric[0] - nx_m)**2 + (stop_metric[1] - ny_m)**2) ** 0.5
    if dist > 500:
        return None

    # Single-source Dijkstra with distance cutoff
    dists = dijkstra(sparse, directed=True, indices=source_idx, limit=distance_m)
    reachable = np.where(np.isfinite(dists))[0]

    if len(reachable) < 3:
        return None
    return reachable


def reachable_edges(reachable, sparse):
    """(i, j) index pairs of edges with both endpoints reachable."""
    reachable_set = set(reachable)
    edges = []
    sub = sparse[reachable]
    for local_i, global_i in enumerate(reachable):
        for j_pos in range(sub.indptr[local_i], sub.indptr[local_i + 1]):
            global_j = sub.indices[j_pos]
            if global_j in reachable_set:
                edges.append((global_i, global_j))
    return edges


def create_isochrone_buffer(reachable, coords_metric, sparse):
    """Classic isochrone: union of BUFFER_M buffers around reachable nodes + edges."""
    reach_coords = coords_metric[reachable]
    node_buffers = [Point(x, y).buffer(BUFFER_M) for x, y in reach_coords]
    edge_buffers = [
        LineString([coords_metric[i], coords_metric[j]]).buffer(BUFFER_M)
        for i, j in reachable_edges(reachable, sparse)
    ]
    return unary_union(node_buffers + edge_buffers)


class BarrierGrid:
    """City-wide raster of OPEN / BUILDING / BARRIER cells in the metric CRS."""

    def __init__(self, city: dict, crs_metric: str, coords_metric: np.ndarray):
        from rasterio.features import rasterize
        from rasterio.transform import from_origin

        osm_dir = city['osm_dir']
        buildings_file = osm_dir / "buildings.gpkg"
        barriers_file = osm_dir / "barriers.gpkg"
        for f in (buildings_file, barriers_file):
            if not f.exists():
                raise FileNotFoundError(
                    f"{f} not found. Fetch it with fetch_osm_basemap.py --city {city['key']}")

        margin = BUFFER_M + 50
        self.x0 = np.floor(coords_metric[:, 0].min() - margin)
        self.y1 = np.ceil(coords_metric[:, 1].max() + margin)
        width = int(np.ceil((coords_metric[:, 0].max() + margin - self.x0) / CELL_M))
        height = int(np.ceil((self.y1 - (coords_metric[:, 1].min() - margin)) / CELL_M))
        transform = from_origin(self.x0, self.y1, CELL_M, CELL_M)
        print(f"Barrier grid: {width:,} x {height:,} cells at {CELL_M:g} m "
              f"({width * height / 1e6:.0f} MB)")

        self.grid = np.zeros((height, width), dtype=np.uint8)

        print("  Rasterizing buildings...", end=' ', flush=True)
        bld = gpd.read_file(buildings_file)
        if 'building' in bld.columns:
            bld = bld[~bld['building'].isin(EXCLUDED_BUILDING_TYPES)]
        bld = bld[bld.geometry.notna()].to_crs(crs_metric)
        rasterize(((g, BUILDING) for g in bld.geometry), out=self.grid,
                  transform=transform)
        print(f"{len(bld):,} buildings")
        del bld

        print("  Rasterizing fences/walls...", end=' ', flush=True)
        bar = gpd.read_file(barriers_file)
        bar = bar[bar.geometry.notna()].to_crs(crs_metric)
        # Closed fence rings come back as polygons; only their outline blocks
        lines = [g.boundary if g.geom_type in ('Polygon', 'MultiPolygon') else g
                 for g in bar.geometry]
        # all_touched keeps lines 4-connected, so diagonal moves can't slip through
        rasterize(((g, BARRIER) for g in lines), out=self.grid,
                  transform=transform, all_touched=True)
        print(f"{len(bar):,} barriers")
        del bar, lines

        self.costs_lookup = np.array([1.0, np.inf, np.inf])

    @classmethod
    def from_array(cls, grid, x0, y1):
        self = cls.__new__(cls)
        self.grid, self.x0, self.y1 = grid, x0, y1
        self.costs_lookup = np.array([1.0, np.inf, np.inf])
        return self

    def window(self, minx, miny, maxx, maxy):
        """Grid slice and its affine transform for a metric bbox."""
        from rasterio.transform import from_origin
        h, w = self.grid.shape
        c0 = max(int((minx - self.x0) // CELL_M), 0)
        c1 = min(int((maxx - self.x0) // CELL_M) + 1, w)
        r0 = max(int((self.y1 - maxy) // CELL_M), 0)
        r1 = min(int((self.y1 - miny) // CELL_M) + 1, h)
        transform = from_origin(self.x0 + c0 * CELL_M, self.y1 - r0 * CELL_M, CELL_M, CELL_M)
        return self.grid[r0:r1, c0:c1], transform


def create_isochrone_barriers(reachable, coords_metric, sparse, grid):
    """
    Barrier-aware isochrone: spread BUFFER_M off the reachable network, but
    fences/walls and buildings block the spread. Buildings are then filled
    in up to BUILDING_PERMEABILITY_M from the reached area (accessible but
    not passable).
    """
    from rasterio.features import rasterize, shapes
    from shapely.geometry import shape
    from skimage.graph import MCP_Geometric

    reach_coords = coords_metric[reachable]
    pad = BUFFER_M + 2 * CELL_M
    minx, miny = reach_coords.min(axis=0) - pad
    maxx, maxy = reach_coords.max(axis=0) + pad
    win, transform = grid.window(minx, miny, maxx, maxy)
    if win.size == 0:
        return None

    # Seed cells: the reachable network itself (passable even where it
    # runs through a building passage or a gate in a fence)
    lines = [LineString([coords_metric[i], coords_metric[j]])
             for i, j in reachable_edges(reachable, sparse)]
    lines += [Point(x, y) for x, y in reach_coords]
    seeds = rasterize(lines, out_shape=win.shape, transform=transform,
                      all_touched=True, dtype=np.uint8).astype(bool)
    if not seeds.any():
        return None

    costs = grid.costs_lookup[win]
    costs[seeds] = 1.0
    max_cells = BUFFER_M / CELL_M
    cum, _ = MCP_Geometric(costs).find_costs(np.argwhere(seeds),
                                             max_cumulative_cost=max_cells)
    reached = cum <= max_cells

    # Buildings: accessible from the reached area, up to N metres deep
    building = win == BUILDING
    if building.any():
        depth = distance_transform_edt(~reached) * CELL_M
        reached |= building & (depth <= BUILDING_PERMEABILITY_M)

    # Fence cells bordering the reached area count as reached, so a fence
    # crossing open ground doesn't cut a 2 m slit (it still blocked the spread)
    barrier = win == BARRIER
    if barrier.any():
        reached |= barrier & binary_dilation(reached, structure=EIGHT)

    # Fill small enclosed holes (building cores, raster pockets)
    holes, _ = label(~reached)
    hole_cells = np.bincount(holes.ravel())
    small = hole_cells * CELL_M ** 2 < MIN_HOLE_M2
    small[0] = False
    border = np.unique(np.r_[holes[0], holes[-1], holes[:, 0], holes[:, -1]])
    small[border] = False
    reached |= small[holes]

    # Drop tiny detached fragments
    parts, _ = label(reached, structure=EIGHT)
    keep = np.bincount(parts.ravel()) * CELL_M ** 2 >= MIN_PART_M2
    keep[0] = False
    reached = keep[parts]
    if not reached.any():
        return None

    polys = [shape(geom) for geom, val in
             shapes(reached.astype(np.uint8), mask=reached, transform=transform,
                    connectivity=8)
             if val == 1]
    if not polys:
        return None
    # Remove raster staircase while keeping 2 m features
    return unary_union(polys).simplify(CELL_M * 0.5, preserve_topology=True)


def compute_stop(nn_idx, sx, sy, ctx):
    """Isochrone(s) for one stop -> (public polygon, residents polygon or None)."""
    coords, grid, gate_net = ctx['coords'], ctx['grid'], ctx['gate_net']
    if gate_net is not None:
        reach = reachable_two_layer(nn_idx, (sx, sy), coords, gate_net, DISTANCE_M)
        if reach is None:
            return None, None
        pub, res = reach
        poly = create_isochrone_barriers(pub, coords, gate_net['public'], grid)
        poly_res = poly if len(res) == 0 else create_isochrone_barriers(
            np.concatenate([pub, res]), coords, gate_net['full'], grid)
        return poly, poly_res
    reachable = reachable_nodes(nn_idx, (sx, sy), coords, ctx['sparse'], DISTANCE_M)
    if reachable is None:
        return None, None
    if grid is not None:
        return create_isochrone_barriers(reachable, coords, ctx['sparse'], grid), None
    return create_isochrone_buffer(reachable, coords, ctx['sparse']), None


def stop_record(stop, poly, poly_res):
    return {
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
        'geometry': poly,
        'geometry_res': poly_res,
    }


_CTX = None  # per-process context: coords, matrices, grid


def process_chunk(task):
    """task = (chunk_start, [(stop_dict, nn_idx, sx, sy), ...]) -> (chunk_start, results, skipped)."""
    chunk_start, records = task
    results, skipped = [], 0
    for stop, nn_idx, sx, sy in records:
        poly, poly_res = compute_stop(nn_idx, sx, sy, _CTX)
        if poly is not None and not poly.is_empty:
            results.append(stop_record(stop, poly, poly_res))
        else:
            skipped += 1
    return chunk_start, results, skipped


def save_shared(shared_dir, ctx):
    """Dump arrays for worker processes (they memory-map them instead of copying)."""
    shared_dir.mkdir(exist_ok=True)
    meta = {'mats': {}, 'grid': None, 'gates': ctx['gate_net'] is not None}
    np.save(shared_dir / 'coords.npy', np.ascontiguousarray(ctx['coords']))
    mats = ({'sparse': ctx['sparse']} if ctx['gate_net'] is None else
            {k: ctx['gate_net'][k] for k in ('public', 'full', 'two_layer')})
    for name, m in mats.items():
        m = m.tocsr()
        m.sort_indices()
        np.save(shared_dir / f'{name}_data.npy', m.data.astype(np.float64))
        np.save(shared_dir / f'{name}_indices.npy', m.indices.astype(np.int32))
        np.save(shared_dir / f'{name}_indptr.npy', m.indptr.astype(np.int32))
        meta['mats'][name] = m.shape
    if ctx['gate_net'] is not None:
        np.save(shared_dir / 'private_node.npy', ctx['gate_net']['private_node'])
    if ctx['grid'] is not None:
        np.save(shared_dir / 'grid.npy', ctx['grid'].grid)
        meta['grid'] = (ctx['grid'].x0, ctx['grid'].y1)
    with open(shared_dir / 'meta.pkl', 'wb') as f:
        pickle.dump(meta, f)


def _init_worker(shared_dir):
    """Worker initializer: memory-map the shared arrays (copy-on-write, shared pages)."""
    global _CTX
    d = Path(shared_dir)
    with open(d / 'meta.pkl', 'rb') as f:
        meta = pickle.load(f)
    load = lambda name: np.load(d / f'{name}.npy', mmap_mode='c')
    mats = {name: csr_matrix((load(f'{name}_data'), load(f'{name}_indices'),
                              load(f'{name}_indptr')), shape=shape)
            for name, shape in meta['mats'].items()}
    grid = BarrierGrid.from_array(load('grid'), *meta['grid']) if meta['grid'] else None
    gate_net = None
    if meta['gates']:
        gate_net = {k: mats[k] for k in ('public', 'full', 'two_layer')}
        gate_net['private_node'] = load('private_node')
    _CTX = {'coords': load('coords'), 'sparse': mats.get('sparse'),
            'gate_net': gate_net, 'grid': grid}


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
    parser.add_argument('--barriers', action='store_true',
                        help='Block off-network spread with fences/walls and buildings')
    parser.add_argument('--gates', action='store_true',
                        help='With --barriers: closed gates / private ways are residents-only '
                             '(outputs isochrones_gates + isochrones_gates_residents)')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                        help=f'Parallel worker processes (default {DEFAULT_WORKERS}; 1 = serial)')
    parser.add_argument('data_folder', nargs='?', help='Data folder (default: most recent)')
    args = parser.parse_args()

    city = get_city(args.city)
    crs_metric = city['crs_metric']
    if args.gates:
        args.barriers = True

    data_dir = Path(args.data_folder) if args.data_folder else find_latest_data_dir(city)
    input_file = data_dir / "stops_trip_count.csv"
    variant = "_gates" if args.gates else "_barriers" if args.barriers else ""
    suffix = variant + ("_sample" if args.sample else "")
    output_file = data_dir / f"isochrones{suffix}.gpkg"
    residents_file = data_dir / f"isochrones{variant}_residents{suffix[len(variant):]}.gpkg"

    print("=" * 60)
    print(f"Isochrone Generator — {city['name']}")
    print("=" * 60)
    print(f"Input:   {input_file}")
    print(f"Output:  {output_file}")
    print(f"CRS:     {crs_metric}")
    print(f"Walking: {TIME_LIMIT} min / {DISTANCE_M:.0f} m at {WALKING_SPEED} km/h")
    mode = (f'{BUFFER_M} m buffer' if not args.barriers else
            'fences/walls + buildings as barriers' + (' + gates (public / residents)' if args.gates else ''))
    print(f"Mode:    {mode}")
    if args.gates:
        print(f"Residents output: {residents_file}")
    print()

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

    access = None
    if args.gates:
        print("Loading access rules...", end=' ', flush=True)
        access = {'closed_gates': closed_gate_ids(city['osm_dir'], crs_metric),
                  'private_ways': private_way_ids(city['osm_dir'])}
        print(f"{len(access['closed_gates']):,} closed gates, "
              f"{len(access['private_ways']):,} private ways")
    node_ids, tree, coords_metric, sparse, node_to_idx, gate_net = convert_to_sparse(
        G, crs_metric, access)

    # Free NetworkX graph to reclaim ~1.5 GB
    del G
    import gc; gc.collect()
    print("  (NetworkX graph freed)\n")

    grid = BarrierGrid(city, crs_metric, coords_metric) if args.barriers else None

    to_metric = Transformer.from_crs("EPSG:4326", crs_metric, always_xy=True)

    # Checkpoints: one pickle per chunk of stops; the folder name encodes the
    # parameters so a change in settings never reuses stale chunks
    params = f"{DISTANCE_M:.0f}_{BUFFER_M}"
    if args.barriers:
        params += f"_c{CELL_M:g}_b{BUILDING_PERMEABILITY_M}_h{MIN_HOLE_M2}_p{MIN_PART_M2}"
    if args.gates:
        params += "_g1"  # bump when access_rules change
    partial_dir = data_dir / f".partial_isochrones{suffix}_{params}"
    partial_dir.mkdir(exist_ok=True)

    # Snap every stop to its network node up front (vectorised)
    lons, lats = stops['stop_lon'].to_numpy(), stops['stop_lat'].to_numpy()
    sxs, sys_ = to_metric.transform(lons, lats)
    if gate_net is not None:
        _, k = gate_net['public_tree'].query(np.column_stack([lons, lats]))
        nns = gate_net['public_idx'][k]  # stops are on public streets
    else:
        _, nns = tree.query(np.column_stack([lons, lats]))
    stop_dicts = stops.to_dict('records')

    chunks = {}
    for chunk_start in range(0, len(stops), CHECKPOINT_EVERY):
        chunk_file = partial_dir / f"chunk_{chunk_start:06d}.pkl"
        if chunk_file.exists():
            with open(chunk_file, 'rb') as f:
                chunks[chunk_start] = pickle.load(f)
    pending = [c for c in range(0, len(stops), CHECKPOINT_EVERY) if c not in chunks]

    def tasks():
        for c in pending:
            idx = range(c, min(c + CHECKPOINT_EVERY, len(stops)))
            yield c, [(stop_dicts[i], int(nns[i]), sxs[i], sys_[i]) for i in idx]

    workers = max(1, min(args.workers, len(pending)))
    print(f"Generating isochrones: {len(pending)} chunks of {CHECKPOINT_EVERY} stops to do "
          f"({len(chunks)} cached), {workers} worker(s); checkpoints in {partial_dir.name}\n",
          flush=True)
    start_time = datetime.now()
    ctx = {'coords': coords_metric, 'sparse': sparse, 'gate_net': gate_net, 'grid': grid}

    def collect(chunk_start, chunk_results, chunk_skipped, n_done):
        with open(partial_dir / f"chunk_{chunk_start:06d}.pkl", 'wb') as f:
            pickle.dump((chunk_results, chunk_skipped), f, protocol=pickle.HIGHEST_PROTOCOL)
        chunks[chunk_start] = (chunk_results, chunk_skipped)
        elapsed = (datetime.now() - start_time).total_seconds()
        remaining = elapsed / n_done * (len(pending) - n_done)
        print(f"  chunk {n_done}/{len(pending)} — {elapsed:.0f}s elapsed, "
              f"~{remaining:.0f}s remaining", flush=True)

    if workers > 1:
        from multiprocessing import Pool
        shared_dir = data_dir / f".shared_isochrones{suffix}"
        save_shared(shared_dir, ctx)
        del ctx, sparse, gate_net, grid  # workers memory-map their own view
        import gc; gc.collect()
        with Pool(workers, initializer=_init_worker, initargs=(str(shared_dir),)) as pool:
            for n, (c, res, sk) in enumerate(pool.imap_unordered(process_chunk, tasks()), 1):
                collect(c, res, sk, n)
        shutil.rmtree(shared_dir, ignore_errors=True)
    else:
        global _CTX
        _CTX = ctx
        for n, task in enumerate(tasks(), 1):
            collect(*process_chunk(task), n)

    results = [r for c in sorted(chunks) for r in chunks[c][0]]
    skipped = sum(chunks[c][1] for c in chunks)

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
    res_geoms = gdf.pop('geometry_res')
    gdf['area_ha'] = gdf.geometry.area / 10000
    gdf.to_crs("EPSG:4326").to_file(output_file, driver="GPKG")
    if args.gates:
        gres = gdf.copy()
        gres['geometry'] = gpd.GeoSeries(res_geoms.fillna(gdf.geometry), crs=crs_metric)
        gres['area_ha'] = gres.geometry.area / 10000
        gres.to_crs("EPSG:4326").to_file(residents_file, driver="GPKG")
    shutil.rmtree(partial_dir)  # checkpoints no longer needed

    print("Done")
    print(f"\n  Avg area: {gdf['area_ha'].mean():.1f} ha")
    print(f"  Saved to: {output_file}")
    if args.gates:
        print(f"  Residents avg area: {gres['area_ha'].mean():.1f} ha")
        print(f"  Saved to: {residents_file}")


if __name__ == "__main__":
    main()
