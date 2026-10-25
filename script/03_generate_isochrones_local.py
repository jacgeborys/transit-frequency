"""
Generate Walking Isochrones for Transit Stops - Local Version
Uses OSMnx walking network + scipy sparse graph to build 5-minute walking polygons.
No API keys or rate limits.

Usage:
    python 03_generate_isochrones_local.py --city warsaw [--sample N] [--barriers] [data_folder]
    python 03_generate_isochrones_local.py --city poznan

    --sample N   Process only the first N stops (for testing)
    --barriers   Off-network spread respects fences/walls (impassable) and
                 buildings (enterable like open space, never crossed or exited).
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
from scipy.sparse.csgraph import dijkstra, connected_components
from scipy.ndimage import distance_transform_edt, binary_dilation, label

from cities import get_city, add_city_argument
from access_rules import closed_gate_ids, private_way_ids

WALKING_SPEED = 4.5   # km/h
TIME_LIMIT = 5        # minutes
DISTANCE_M = TIME_LIMIT * (WALKING_SPEED * 1000 / 60)  # 375 m
BUFFER_M = 50         # buffer around nodes and edges

# Barrier mode
CELL_M = 2.0                      # raster resolution
BUILDING_RULE = 'oneway'          # buildings: enterable like open space, never exited
EXCLUDED_BUILDING_TYPES = {'roof', 'carport'}  # open structures, walkable underneath
MIN_HOLE_M2 = 200                 # enclosed holes smaller than this are filled
MIN_PART_M2 = 50                  # detached fragments smaller than this are dropped
OPEN, BUILDING, BARRIER, GATE = 0, 1, 2, 3  # GATE: closed gate in a fence, residents only
ISLAND_MAX_NODES = 5000           # public path islands reachable only via private edges
ISLAND_STOP_M = 30                # a stop this close to an island path also walks inside it
# Slope-aware walking (cities with 'hills': True and a DEM from fetch_dem.py)
DEM_SAMPLE_M = 10.0               # elevation sampled along every edge at this spacing
STEPS_MIN_SLOPE = 0.25            # stairs: terrain models smooth them out, count at least this
MAX_SLOPE = 1.0                   # clamp DEM noise (cliffs, retaining walls)
GATE_RADIUS_M = 2.5               # opening punched into the fence around a gate node
EIGHT = np.ones((3, 3), dtype=bool)
# Raster cost per cell value (OPEN, BUILDING, BARRIER, GATE)
COSTS_PUBLIC = np.array([1.0, np.inf, np.inf, np.inf])
COSTS_RESIDENTS = np.array([1.0, np.inf, np.inf, 1.0])
# Major roads and railways as barriers (--major-barriers, 2026-10-25): a major road can only
# be crossed at a mapped crossing, a signal-controlled junction or where a footway/path goes
# through the node; elsewhere each side of the road is its own sidewalk. Railways block the
# off-network spread except where a walkable way crosses them.
MAJOR_HIGHWAYS = {'motorway', 'motorway_link', 'trunk', 'trunk_link', 'primary', 'primary_link',
                  'secondary', 'secondary_link'}
PATH_HIGHWAYS = {'footway', 'path', 'pedestrian', 'steps', 'corridor', 'crossing'}
BARRIER_RAILWAYS = {'rail', 'narrow_gauge', 'light_rail', 'monorail'}  # not tram, not subway
SIDE_OFFSET_M = 5.0               # sidewalk copies of a road node sit this far off its centre line
CROSSING_RADIUS_M = 3.0           # opening in the road/rail barrier at a crossing

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


def _csr_min(weights, rows, cols, n):
    """Sparse matrix keeping the SHORTEST of duplicate (row, col) entries.

    csr_matrix((w, (r, c))) adds duplicates up. The network has copies of the same way
    (02 merged its download tiles, and a way crossing a tile border came back from each
    tile: 6.8 % of New York's node pairs, up to 4 copies), so such streets were routed at
    2-4x their length until 2026-10-10. Parallel ways between the same two nodes: the
    shorter one is what a walker uses.
    """
    weights, rows, cols = (np.asarray(a) for a in (weights, rows, cols))
    if len(rows) == 0:
        return csr_matrix((n, n))
    order = np.lexsort((weights, cols, rows))
    r, c, w = rows[order], cols[order], weights[order]
    first = np.ones(len(r), bool)
    first[1:] = (r[1:] != r[:-1]) | (c[1:] != c[:-1])
    return csr_matrix((w[first], (r[first], c[first])), shape=(n, n))


def tobler_factor(slope):
    """Walking time on this slope relative to flat ground, the same both ways: the mean of the
    uphill and downhill time from Tobler's hiking function v = 6 exp(-3.5 |s + 0.05|) km/h."""
    s = np.minimum(np.abs(slope), MAX_SLOPE)
    v0 = np.exp(-3.5 * 0.05)
    return 0.5 * (v0 / np.exp(-3.5 * np.abs(s + 0.05)) + v0 / np.exp(-3.5 * np.abs(0.05 - s)))


def load_dem(city):
    """(elevation array, rasterio transform) for a city with 'hills', or None."""
    if not city.get('hills'):
        return None
    path = Path(r'D:\QGIS\osm_basemap\dem') / f"{city['key']}.tif"
    if not path.exists():
        print(f"WARNING: {city['name']} has 'hills' but no {path} (run fetch_dem.py): walking stays flat")
        return None
    import rasterio
    with rasterio.open(path) as d:
        a = d.read(1).astype('float32')
        if d.nodata is not None and not np.isnan(d.nodata):
            a[a == d.nodata] = np.nan
        return a, d.transform


def hill_weights(lengths, a_xy, b_xy, highway, structure, dem):
    """Edge cost = sum over ~10 m pieces of piece length x tobler_factor(piece slope)."""
    from scipy.ndimage import map_coordinates
    arr, tr = dem
    n_p = np.maximum(1, np.ceil(lengths / DEM_SAMPLE_M)).astype(np.int64)
    edge = np.repeat(np.arange(len(lengths)), n_p)
    k = np.arange(len(edge)) - np.repeat(np.cumsum(n_p) - n_p, n_p)
    t0, t1 = k / n_p[edge], (k + 1) / n_p[edge]
    inv = ~tr

    def z(t):
        x = a_xy[edge, 0] + (b_xy[edge, 0] - a_xy[edge, 0]) * t
        y = a_xy[edge, 1] + (b_xy[edge, 1] - a_xy[edge, 1]) * t
        col, row = inv * (x, y)
        return map_coordinates(arr, [row - 0.5, col - 0.5], order=1, mode='nearest', cval=np.nan)

    piece = lengths[edge] / n_p[edge]
    with np.errstate(invalid='ignore', divide='ignore'):
        slope = np.where(piece > 0, (z(t1) - z(t0)) / piece, 0.0)
    slope = np.nan_to_num(slope)
    steps = np.asarray([h == 'steps' for h in highway])[edge]
    slope = np.where(steps, np.maximum(np.abs(slope), STEPS_MIN_SLOPE), slope)
    f = tobler_factor(slope)
    flat = np.asarray([st in ('bridge', 'tunnel') for st in structure])[edge]
    f = np.where(flat, 1.0, f)
    w = np.bincount(edge, weights=piece * f, minlength=len(lengths))
    print(f"  Hills: {len(edge):,} pieces sampled; edge cost x{w.sum() / lengths.sum():.3f} on average, "
          f"{(w > 1.25 * lengths).mean() * 100:.1f} % of edges over +25 %")
    return w


def island_starts(gate_net, coords, sx, sy):
    """Per stop: an island node within ISLAND_STOP_M (it also walks inside that island), else -1."""
    lab = gate_net['island_lab']
    isl = np.where(lab >= 0)[0]
    if not len(isl):
        return np.full(len(sx), -1)
    d, k = cKDTree(coords[isl]).query(np.column_stack([sx, sy]))
    return np.where(d <= ISLAND_STOP_M, isl[k], -1)


def split_major_roads(rows, cols, weights, private, highway, crossing, coords):
    """
    Major roads become barriers in the network: every node on a major road that is not a
    crossing is split into one copy per side ("sector" between the major edges meeting
    there). Side streets attach to the copy on their side, and the road itself carries one
    edge per side (left sidewalk to left sidewalk). A node is crossable if mapped as a
    crossing / traffic signals, or if footway-class edges leave it on different sides.
    Input edges are directed, both directions present (as in G).
    Returns new (rows, cols, weights, private), the original node of every node (copies
    appended after the originals), their metric coordinates (copies SIDE_OFFSET_M off the
    centre line, into their sector) and the crossable nodes on major roads.
    """
    rows, cols = np.asarray(rows), np.asarray(cols)
    n = len(coords)
    major = np.array([h in MAJOR_HIGHWAYS for h in highway])
    path = np.array([h in PATH_HIGHWAYS for h in highway])
    d = coords[cols] - coords[rows]
    ang = np.arctan2(d[:, 1], d[:, 0])
    order = np.lexsort((ang, rows))
    starts = np.searchsorted(rows[order], np.arange(n + 1))
    first = {}
    for e, uv in enumerate(zip(rows.tolist(), cols.tolist())):
        first.setdefault(uv, e)
    rev_e = np.array([first[(v, u)] for u, v in zip(rows.tolist(), cols.tolist())])
    sec = np.full(len(rows), -1)        # sector of a non-major edge at its row node
    ccw = np.full(len(rows), -1)        # sectors left / right of a major edge at its row node
    cw = np.full(len(rows), -1)
    copy_of = {}                        # (node, sector) -> node index (sector 0 keeps the node)
    new_xy, new_orig, offsets, crossable = [], [], {}, []
    for u in np.where(np.bincount(rows[major], minlength=n) >= 2)[0]:
        es_all = order[starts[u]:starts[u + 1]]
        # parallel major edges to the same neighbour (two ways on one segment) count once
        seen, es = {}, []
        for e in es_all:
            if major[e]:
                if cols[e] in seen:
                    continue
                seen[cols[e]] = e
            es.append(e)
        es = np.array(es)
        is_m = major[es]
        k = int(is_m.sum())
        if k < 2:
            continue
        if crossing[u]:
            crossable.append(u)
            continue
        s_non = (np.cumsum(is_m) - 1) % k      # non-major edge: sector CCW of the last major
        if len({int(s) for s, e in zip(s_non, es) if not major[e] and path[e]}) >= 2:
            crossable.append(u)                # a footway goes through: crossing point
            continue
        majors = es[is_m]
        for j, e in enumerate(majors):
            ccw[e], cw[e] = j, (j - 1) % k
        for e, sj, m in zip(es, s_non, is_m):
            if not m:
                sec[e] = sj
        for e in es_all:                       # parallel duplicates: sectors of their twin
            if major[e] and ccw[e] < 0:
                t = seen[cols[e]]
                ccw[e], cw[e] = ccw[t], cw[t]
        a = ang[majors]
        for j in range(k):
            mid = a[j] + ((a[(j + 1) % k] - a[j]) % (2 * np.pi)) / 2
            off = coords[u] + SIDE_OFFSET_M * np.array([np.cos(mid), np.sin(mid)])
            if j == 0:
                offsets[u] = off
            else:
                copy_of[(u, j)] = n + len(new_xy)
                new_xy.append(off)
                new_orig.append(u)

    def node(u, s):
        return copy_of.get((u, s), u) if s > 0 else u

    R, C, W, P = [], [], [], []
    for e in range(len(rows)):
        u, v, f = rows[e], cols[e], rev_e[e]
        p = private[e] if private is not None else False
        if major[e]:
            # left of u->v = CCW of the edge at u = CW of the reverse edge at v
            for su, sv in ((ccw[e], cw[f]), (cw[e], ccw[f])):
                R.append(node(u, su)); C.append(node(v, sv)); W.append(weights[e]); P.append(p)
        else:
            R.append(node(u, sec[e])); C.append(node(v, sec[f])); W.append(weights[e]); P.append(p)
    xy = coords.copy()
    for u, off in offsets.items():
        xy[u] = off
    xy = np.vstack([xy, np.array(new_xy).reshape(-1, 2)])
    orig = np.concatenate([np.arange(n), np.array(new_orig, dtype=int)])
    return R, C, W, P, orig, xy, np.array(crossable, dtype=int)


def convert_to_sparse(G, crs_metric: str, access=None, dem=None, major=False):
    """
    Convert NetworkX graph to scipy sparse matrix + coordinate arrays.
    Returns: (node_ids, KDTree, coords_metric, sparse_matrix, node_to_idx, gate_net)
    gate_net is None unless access = {'closed_gates': set, 'private_ways': set}.
    The NetworkX graph can be freed after this to save memory.
    major=True (needs access): major roads are crossed only at crossings (split_major_roads);
    node_ids then has one entry per node incl. the sidewalk copies (node_to_idx: originals),
    and gate_net['major'] holds the road/rail barrier input for BarrierGrid.
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
    private, highway, structure = [], [], []
    for u, v, data in G.edges(data=True):
        ui, vi = node_to_idx[u], node_to_idx[v]
        length = data.get('length', 0)
        rows.append(ui)
        cols.append(vi)
        weights.append(length)
        if dem is not None or major:
            highway.append(data.get('highway'))
            structure.append(data.get('structure'))
        if access is not None:
            osmid = data.get('osmid')
            way_ids = osmid if isinstance(osmid, list) else [osmid]
            private.append(u in access['closed_gates'] or v in access['closed_gates']
                           or any(_as_int(w) in access['private_ways'] for w in way_ids))

    # Walking goes both ways on every edge: 02 used to keep oneway=yes (one-way streets,
    # Dutch one-way cycle paths) one-directional, so 8-9 % of the edges could only be walked
    # one way and stops on them reached little or nothing (until 2026-10-11)
    n_dir = len(rows)
    if dem is not None:   # walking time on slopes, as a flat-equivalent distance (same both ways)
        weights = list(hill_weights(np.asarray(weights, float), coords_metric[rows],
                                    coords_metric[cols], highway, structure, dem))
    node_ids = np.array(nids)
    lonlats = np.column_stack([lons, lats])
    major_info = None
    if major:
        if not G.graph.get('has_crossings'):
            raise RuntimeError("--major-barriers needs a walking network with crossings: "
                               "rebuild it with 02_fetch_walking_network.py (from its tile cache)")
        crossing = np.array([bool(G.nodes[nd].get('crossing')) for nd in nids])
        orig_xy = coords_metric
        r_, c_ = np.asarray(rows), np.asarray(cols)
        und = r_ < c_
        ground = und & np.array([s is None for s in structure])
        road = ground & np.array([h in MAJOR_HIGHWAYS for h in highway])
        rows, cols, weights, private, orig, coords_metric, crossable = split_major_roads(
            rows, cols, weights, private if access is not None else None, highway, crossing,
            coords_metric)
        major_info = {
            'road_segs': np.stack([orig_xy[r_[road]], orig_xy[c_[road]]], axis=1),
            'walk_segs': np.stack([orig_xy[r_[ground]], orig_xy[c_[ground]]], axis=1),
            'crossings': orig_xy[crossable],
        }
        n_split = len(orig) - n_nodes
        n_nodes = len(orig)
        node_ids = node_ids[orig]
        to_ll = Transformer.from_crs(crs_metric, "EPSG:4326", always_xy=True)
        lonlats = np.column_stack(to_ll.transform(coords_metric[:, 0], coords_metric[:, 1]))
        print(f"\n  Major roads: {n_split:,} sidewalk copies of road nodes, "
              f"{len(crossable):,} crossing points, {road.sum():,} road segments",
              end='\n  ', flush=True)
    rows, cols = rows + cols, cols + rows
    weights = weights + weights
    if access is not None:
        private = private + private
    sparse = _csr_min(weights, rows, cols, n_nodes)

    tree = cKDTree(lonlats)

    print(f"Done ({n_nodes:,} nodes, {n_dir:,} edges, walkable both ways)")
    if access is None:
        return node_ids, tree, coords_metric, sparse, node_to_idx, None

    # Public / private split for gate-aware routing
    rows, cols, weights, private = map(np.asarray, (rows, cols, weights, private))
    # Public "islands": untagged paths behind closed gates (stadium grounds, gated
    # estates) don't connect to the main public network. Nobody from outside can
    # use them, and residents need them -> treat them as private.
    pub = ~private
    _, lab = connected_components(_csr_min(weights[pub], rows[pub], cols[pub], n_nodes),
                                  directed=False)
    on_pub = np.zeros(n_nodes, bool)
    on_pub[rows[pub]] = True
    on_pub[cols[pub]] = True
    sizes = np.bincount(lab[on_pub], minlength=lab.max() + 1)
    island = (sizes < ISLAND_MAX_NODES) & (np.arange(len(sizes)) != sizes.argmax())
    island_lab = np.where(on_pub & island[lab], lab, -1).astype(np.int32)
    to_private = pub & island[lab[rows]]
    private = private | to_private
    print(f"  Public islands behind gates: {to_private.sum():,} edges reclassified private")
    pub = ~private
    sparse_public = _csr_min(weights[pub], rows[pub], cols[pub], n_nodes)
    sparse_private = _csr_min(weights[private], rows[private], cols[private], n_nodes)
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
        'full': sparse,  # all edges, shortest per node pair (a sum would add parallel ways)
        'two_layer': two_layer,
        'private_node': ~has_public,
        'public_tree': cKDTree(lonlats[has_public]),
        'public_idx': np.where(has_public)[0],
        'island_lab': island_lab,   # island id per node (-1: not in a reclassified island)
        'major': major_info,        # road/rail barrier input for BarrierGrid (None: off)
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

    def __init__(self, city: dict, crs_metric: str, coords_metric: np.ndarray,
                 closed_gates=None, major=None):
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

        # Gates mode: gates punch openings into fences, also where no mapped path
        # runs through them (front gardens, fenced plots). Open gates for everyone,
        # closed gates only in the residents run.
        gates_file = osm_dir / "gates.gpkg"
        if closed_gates is not None and gates_file.exists():
            print("  Opening gates in fences...", end=' ', flush=True)
            gates = gpd.read_file(gates_file).to_crs(crs_metric)
            closed = gates.osm_id.astype('int64').isin(closed_gates).to_numpy()
            fence = self.grid == BARRIER
            for value, sel in ((OPEN, ~closed), (GATE, closed)):
                if sel.any():
                    mask = rasterize(((g, 1) for g in gates.geometry[sel].buffer(GATE_RADIUS_M)),
                                     out_shape=self.grid.shape, transform=transform,
                                     all_touched=True, dtype=np.uint8).astype(bool)
                    self.grid[mask & fence] = value
                    del mask
            print(f"{(~closed).sum():,} open, {closed.sum():,} closed")
            del gates, fence

        if major is not None:
            self._add_major_barriers(city, crs_metric, major, transform)
        self.costs_lookup = COSTS_PUBLIC

    def _add_major_barriers(self, city, crs_metric, major, transform):
        """Major roads (at ground level) and railways block the off-network spread like fences,
        except at openings: road crossings, and wherever a walkable way meets the tracks.
        Only open ground becomes barrier (fences, gates and buildings stay as they are)."""
        import shapely
        from rasterio.features import rasterize
        print("  Rasterizing major roads + railways...", end=' ', flush=True)
        roads = list(shapely.linestrings(major['road_segs']))
        rails = []
        rail_file = city['osm_dir'] / 'railways.gpkg'
        if rail_file.exists():
            r = gpd.read_file(rail_file)
            r = r[r['railway'].isin(BARRIER_RAILWAYS) & r['bridge'].isna() & r['tunnel'].isna()]
            if 'layer' in r.columns:
                r = r[pd.to_numeric(r['layer'], errors='coerce').fillna(0) >= 0]
            rails = [g for g in r.to_crs(crs_metric).geometry if g is not None]
        line = rasterize(((g, 1) for g in roads + rails), out_shape=self.grid.shape,
                         transform=transform, all_touched=True, dtype=np.uint8).astype(bool)
        pts = list(shapely.points(major['crossings']))
        if rails:
            walk = shapely.linestrings(major['walk_segs'])
            tree = shapely.STRtree(walk)
            for g in rails:
                hit = tree.query(g, predicate='intersects')
                if len(hit):
                    pts += [p for p in shapely.get_parts(shapely.intersection(walk[hit], g))
                            if not p.is_empty]
        if pts:
            line &= ~rasterize(((p.buffer(CROSSING_RADIUS_M), 1) for p in pts),
                               out_shape=self.grid.shape, transform=transform,
                               all_touched=True, dtype=np.uint8).astype(bool)
        self.grid[line & (self.grid == OPEN)] = BARRIER
        print(f"{len(roads):,} road segments, {len(rails):,} rail lines, {len(pts):,} openings")

    @classmethod
    def from_array(cls, grid, x0, y1):
        self = cls.__new__(cls)
        self.grid, self.x0, self.y1 = grid, x0, y1
        self.costs_lookup = COSTS_PUBLIC
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


def _window_for(reachable, coords_metric, grid):
    reach_coords = coords_metric[reachable]
    pad = BUFFER_M + 2 * CELL_M
    minx, miny = reach_coords.min(axis=0) - pad
    maxx, maxy = reach_coords.max(axis=0) + pad
    return grid.window(minx, miny, maxx, maxy)


def create_isochrone_barriers(reachable, coords_metric, sparse, grid, residents=False):
    """
    Barrier-aware isochrone: spread BUFFER_M off the reachable network, but
    fences/walls and buildings block the spread. Buildings are then filled
    in like open space but never exited again (accessible but
    not passable). residents=True also lets the spread through closed gates.
    """
    from rasterio.features import rasterize, shapes
    from shapely.geometry import shape
    from skimage.graph import MCP_Geometric

    reach_coords = coords_metric[reachable]
    win, transform = _window_for(reachable, coords_metric, grid)
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

    costs = (COSTS_RESIDENTS if residents else grid.costs_lookup)[win]
    # Path cells are passable (passages through buildings) - but never on a fence/wall
    # cell: a sidewalk running along a fence would otherwise punch holes in it
    # (closed gates count as fence for the public run, same reason)
    fence = (win == BARRIER) | ((win == GATE) & (not residents))
    costs[seeds & ~fence] = 1.0
    max_cells = BUFFER_M / CELL_M
    cum, _ = MCP_Geometric(costs).find_costs(np.argwhere(seeds),
                                             max_cumulative_cost=max_cells)
    reached = cum <= max_cells

    # Buildings are one-way space: the walk spreads inside them like open ground
    # (same budget), but can't come back out, so a building is never a shortcut.
    # Outside cells come only from the run above (buildings impassable); building
    # cells from a second run where buildings are passable.
    building = win == BUILDING
    if building.any():
        costs_in = np.where(fence, np.inf, 1.0)
        cum_in, _ = MCP_Geometric(costs_in).find_costs(np.argwhere(seeds),
                                                       max_cumulative_cost=max_cells)
        reached |= building & (cum_in <= max_cells)

    # Fence cells bordering the reached area count as reached, so a fence
    # crossing open ground doesn't cut a 2 m slit (it still blocked the spread)
    barrier = (win == BARRIER) | (win == GATE)
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


def island_reach(isl_idx, coords, gate_net, grid):
    """Walk inside the island a stop sits in (JFK's terminals: reached from the city only via
    private airport roads, but the AirTrain stops inside). Added to the stop's normal isochrone,
    never replacing it (disconnected station mezzanines are islands too)."""
    lab = gate_net['island_lab']
    d = dijkstra(gate_net['full'], directed=True, indices=isl_idx, limit=DISTANCE_M)
    nodes = np.where(np.isfinite(d) & (lab == lab[isl_idx]))[0]
    if len(nodes) < 3:
        return None
    return create_isochrone_barriers(nodes, coords, gate_net['full'], grid)


def _union(a, b):
    if a is None or a.is_empty:
        return b
    return a if b is None or b.is_empty else a.union(b)


def compute_stop(nn_idx, sx, sy, ctx, isl_idx=-1):
    """Isochrone(s) for one stop -> (public polygon, residents polygon or None)."""
    coords, grid, gate_net = ctx['coords'], ctx['grid'], ctx['gate_net']
    if gate_net is not None:
        poly = poly_res = None
        reach = reachable_two_layer(nn_idx, (sx, sy), coords, gate_net, DISTANCE_M)
        if reach is not None:
            pub, res = reach
            poly = create_isochrone_barriers(pub, coords, gate_net['public'], grid)
            # Residents run: when private paths were reached or closed gates are nearby
            if len(res) == 0 and not (_window_for(pub, coords, grid)[0] == GATE).any():
                poly_res = poly
            else:
                poly_res = create_isochrone_barriers(
                    np.concatenate([pub, res]), coords, gate_net['full'], grid, residents=True)
        if isl_idx >= 0 and grid is not None:
            inner = island_reach(isl_idx, coords, gate_net, grid)
            poly, poly_res = _union(poly, inner), _union(poly_res, inner)
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


def set_minutes(minutes):
    """Walking time budget (default 5 min = 375 m); workers get it via the shared meta."""
    global TIME_LIMIT, DISTANCE_M
    TIME_LIMIT = minutes
    DISTANCE_M = TIME_LIMIT * (WALKING_SPEED * 1000 / 60)


def process_chunk(task):
    """task = (chunk_start, [(stop_dict, nn_idx, sx, sy), ...]) -> (chunk_start, results, skipped)."""
    chunk_start, records = task
    results, skipped = [], 0
    for stop, nn_idx, sx, sy, isl_idx in records:
        poly, poly_res = compute_stop(nn_idx, sx, sy, _CTX, isl_idx)
        if poly is not None and not poly.is_empty:
            results.append(stop_record(stop, poly, poly_res))
        else:
            skipped += 1
    return chunk_start, results, skipped


def save_shared(shared_dir, ctx):
    """Dump arrays for worker processes (they memory-map them instead of copying)."""
    shared_dir.mkdir(exist_ok=True)
    meta = {'mats': {}, 'grid': None, 'gates': ctx['gate_net'] is not None,
            'time_limit': TIME_LIMIT}
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
        np.save(shared_dir / 'island_lab.npy', ctx['gate_net']['island_lab'])
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
    set_minutes(meta.get('time_limit', TIME_LIMIT))
    load = lambda name: np.load(d / f'{name}.npy', mmap_mode='c')
    mats = {name: csr_matrix((load(f'{name}_data'), load(f'{name}_indices'),
                              load(f'{name}_indptr')), shape=shape)
            for name, shape in meta['mats'].items()}
    grid = BarrierGrid.from_array(load('grid'), *meta['grid']) if meta['grid'] else None
    gate_net = None
    if meta['gates']:
        gate_net = {k: mats[k] for k in ('public', 'full', 'two_layer')}
        gate_net['private_node'] = load('private_node')
        gate_net['island_lab'] = load('island_lab')
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
    parser.add_argument('--major-barriers', action='store_true',
                        help='Implies --gates: major roads (secondary and up) crossable only at '
                             'crossings, railways only where a way crosses them (output tag _mb)')
    parser.add_argument('--minutes', type=float, default=TIME_LIMIT,
                        help=f'Walking time (default {TIME_LIMIT}; other values add _<N>min to the output)')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                        help=f'Parallel worker processes (default {DEFAULT_WORKERS}; 1 = serial)')
    parser.add_argument('data_folder', nargs='?', help='Data folder (default: most recent)')
    args = parser.parse_args()
    _rt = Path(__file__).resolve().parent / 'runtime.json'  # machine-level overrides
    if _rt.exists():
        import json
        _cfg = json.loads(_rt.read_text(encoding='utf-8'))
        args.workers = max(args.workers, int(_cfg.get('min_workers', 0)))
        if 'min_free_gb' in _cfg:
            globals()['MIN_FREE_GB'] = float(_cfg['min_free_gb'])
        print(f"runtime.json: workers={args.workers}, min_free_gb={_cfg.get('min_free_gb', '-')}")

    city = get_city(args.city)
    crs_metric = city['crs_metric']
    if args.major_barriers:
        args.gates = True
    if args.gates:
        args.barriers = True
    set_minutes(args.minutes)

    data_dir = Path(args.data_folder) if args.data_folder else find_latest_data_dir(city)
    input_file = data_dir / "stops_trip_count.csv"
    variant = "_gates" if args.gates else "_barriers" if args.barriers else ""
    tag = ("_mb" if args.major_barriers else "") + (
        f"_{args.minutes:g}min" if args.minutes != 5 else "")
    suffix = variant + tag + ("_sample" if args.sample else "")
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
    dem = load_dem(city)
    node_ids, tree, coords_metric, sparse, node_to_idx, gate_net = convert_to_sparse(
        G, crs_metric, access, dem, major=args.major_barriers)

    # Free NetworkX graph to reclaim ~1.5 GB
    del G
    import gc; gc.collect()
    print("  (NetworkX graph freed)\n")

    grid = BarrierGrid(city, crs_metric, coords_metric,
                       access['closed_gates'] if access else None,
                       major=gate_net['major'] if gate_net is not None else None) if args.barriers else None
    if gate_net is not None:
        gate_net['major'] = None   # segments were only needed for the grid

    to_metric = Transformer.from_crs("EPSG:4326", crs_metric, always_xy=True)

    # Checkpoints: one pickle per chunk of stops; the folder name encodes the
    # parameters so a change in settings never reuses stale chunks
    params = f"{DISTANCE_M:.0f}_{BUFFER_M}"
    if args.barriers:
        params += f"_c{CELL_M:g}_b{BUILDING_RULE}_h{MIN_HOLE_M2}_p{MIN_PART_M2}"
    if args.gates:
        params += "_g10"  # bump when access_rules change (g7: duplicates not summed; g8: edges two-way; g10: island walks for stops inside)
    if dem is not None:
        params += f"_hill{DEM_SAMPLE_M:g}"
    if args.major_barriers:
        params += f"_mb1_o{SIDE_OFFSET_M:g}_r{CROSSING_RADIUS_M:g}"
    partial_dir = data_dir / f".partial_isochrones{suffix}_{params}"
    partial_dir.mkdir(exist_ok=True)

    # Snap every stop to its network node up front (vectorised)
    lons, lats = stops['stop_lon'].to_numpy(), stops['stop_lat'].to_numpy()
    sxs, sys_ = to_metric.transform(lons, lats)
    if gate_net is not None:
        _, k = gate_net['public_tree'].query(np.column_stack([lons, lats]))
        nns = gate_net['public_idx'][k]  # stops are on public streets
        isls = island_starts(gate_net, coords_metric, sxs, sys_)
        print(f"Stops also walking inside an island (JFK-like): {(isls >= 0).sum()}")
    else:
        _, nns = tree.query(np.column_stack([lons, lats]))
        isls = np.full(len(stops), -1)
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
            yield c, [(stop_dicts[i], int(nns[i]), sxs[i], sys_[i], int(isls[i])) for i in idx]

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
