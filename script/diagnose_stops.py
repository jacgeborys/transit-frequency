"""
Diagnose how every stop enters the walking network, to catch access-model surprises early.

For each stop (same rules and thresholds as 03 --gates):
  snap_m          distance to the public node its walk starts from (03 snaps to the nearest
                  public node, up to 500 m; beyond that the stop is skipped)
  near_m, near    distance to the nearest network node of any kind and what it is:
                  'public', 'island' (public paths reclassified private: they reach the city only
                  through private ways / closed gates, or not at all) or 'private'
  island_*        for stops whose nearest node is in an island: its size and its "doors"
                  (private ways and closed gates touching it). 0 doors = a disconnected bit of
                  mapping (e.g. a station mezzanine whose stairs don't meet the street)
  iso_ha          public isochrone area in the current run (0 = skipped)
Flags: SKIPPED, SNAP_FAR (> 100 m), IN_GATED_ISLAND (island with doors: the stop's own area is
treated as closed, e.g. JFK's terminals behind private airport roads), IN_LOOSE_ISLAND (island
without doors: usually harmless, the walk starts on the street), SMALL_ISO (< 5 ha).

Writes <data>/diagnostics/stops_access.csv and prints a summary; with --stop REGEX also a sketch
per matching stop group: png/previews/diag_<city>_<name>.png (island paths cyan, private orange,
public grey, stop star, dashed red line to where its walk starts, isochrone outline).

Usage:
    python diagnose_stops.py --city newyork [data_folder] [--stop "^Terminal 4$|^Times Sq-42 St$"]
"""
import argparse
import importlib.util
import pickle
import re
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from pyproj import Transformer
from scipy.sparse.csgraph import connected_components
from scipy.spatial import cKDTree

from access_rules import closed_gate_ids, private_way_ids
from cities import get_city, add_city_argument

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('iso', HERE / '03_generate_isochrones_local.py')
iso = importlib.util.module_from_spec(spec)
spec.loader.exec_module(iso)

SNAP_FAR_M = 100
SMALL_ISO_HA = 5
IN_ISLAND_M = 30


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    add_city_argument(ap)
    ap.add_argument('data_folder', nargs='?')
    ap.add_argument('--stop', help='Regex on stop names: also draw sketches for these')
    ap.add_argument('--radius', type=float, default=450, help='Sketch half-width in m')
    args = ap.parse_args()
    city = get_city(args.city)
    crs, osm = city['crs_metric'], city['osm_dir']
    data = Path(args.data_folder) if args.data_folder else sorted(
        d for d in city['data_dir'].iterdir() if (d / 'isochrones_gates.gpkg').exists())[-1]
    out_dir = data / 'diagnostics'
    out_dir.mkdir(exist_ok=True)

    print("Loading network and access rules...", flush=True)
    with open(city['network_dir'] / 'walking_network.pkl', 'rb') as f:
        G = pickle.load(f)
    access = {'closed_gates': closed_gate_ids(osm, crs), 'private_ways': private_way_ids(osm)}
    node_ids, _, coords, _, _, gate_net = iso.convert_to_sparse(G, crs, access)
    n = len(node_ids)

    # The island step of 03, repeated to know *which* island each node belongs to
    idx = {nid: i for i, nid in enumerate(node_ids.tolist())}
    r, c, priv_way, gate_edge = [], [], [], []
    for u, v, d in G.edges(data=True):
        r.append(idx[u])
        c.append(idx[v])
        osmid = d.get('osmid')
        ways = osmid if isinstance(osmid, list) else [osmid]
        priv_way.append(any(iso._as_int(w) in access['private_ways'] for w in ways))
        gate_edge.append(u in access['closed_gates'] or v in access['closed_gates'])
    del G
    r, c, priv_way, gate_edge = map(np.asarray, (r, c, priv_way, gate_edge))
    pub = ~(priv_way | gate_edge)
    _, lab = connected_components(iso._csr_min(np.ones(pub.sum()), r[pub], c[pub], n), directed=False)
    on_pub = np.zeros(n, bool)
    on_pub[r[pub]] = True
    on_pub[c[pub]] = True
    sizes = np.bincount(lab[on_pub], minlength=lab.max() + 1)
    island = (sizes < iso.ISLAND_MAX_NODES) & (np.arange(len(sizes)) != sizes.argmax())
    node_island = np.where(on_pub & island[lab], lab, -1)
    kind = np.where(node_island >= 0, 'island', np.where(gate_net['private_node'], 'private', 'public'))

    stops = pd.read_csv(data / 'stops_trip_count.csv', dtype={'stop_id': str})
    t = Transformer.from_crs('EPSG:4326', crs, always_xy=True)
    sx, sy = t.transform(stops['stop_lon'].to_numpy(), stops['stop_lat'].to_numpy())
    sxy = np.column_stack([sx, sy])
    # Where 03 starts the walk: nearest public node (03 queries in lon/lat, same choice)
    _, k = gate_net['public_tree'].query(np.column_stack([stops['stop_lon'], stops['stop_lat']]))
    snap_idx = gate_net['public_idx'][k]
    snap_m = np.hypot(*(coords[snap_idx] - sxy).T)
    near_m, near_idx = cKDTree(coords).query(sxy)

    isos = gpd.read_file(data / 'isochrones_gates.gpkg').to_crs(crs)
    area = dict(zip(isos['stop_id'].astype(str), isos.geometry.area / 1e4))
    stops['snap_m'] = snap_m.round(1)
    stops['near_m'] = near_m.round(1)
    stops['near'] = kind[near_idx]
    stops['island_id'] = np.where(near_m <= IN_ISLAND_M, node_island[near_idx], -1)
    stops['iso_ha'] = stops['stop_id'].map(area).fillna(0).round(1)

    # Doors of the islands that stops sit in
    doors = {}
    for li in sorted(set(stops['island_id']) - {-1}):
        touch = (node_island[r] == li) | (node_island[c] == li)
        doors[li] = (int((touch & priv_way).sum()), int((touch & gate_edge).sum()), int(sizes[li]))
    stops['island_nodes'] = stops['island_id'].map(lambda i: doors[i][2] if i in doors else 0)
    stops['island_private_doors'] = stops['island_id'].map(lambda i: doors[i][0] if i in doors else 0)
    stops['island_gate_doors'] = stops['island_id'].map(lambda i: doors[i][1] if i in doors else 0)

    def flags(row):
        f = []
        if row.iso_ha == 0:
            f.append('SKIPPED')
        elif row.iso_ha < SMALL_ISO_HA:
            f.append('SMALL_ISO')
        if row.snap_m > SNAP_FAR_M:
            f.append('SNAP_FAR')
        if row.island_id >= 0:
            f.append('IN_GATED_ISLAND' if row.island_private_doors + row.island_gate_doors
                     else 'IN_LOOSE_ISLAND')
        return ' '.join(f)
    stops['flags'] = stops.apply(flags, axis=1)
    cols = ['stop_id', 'stop_name', 'trip_count', 'flags', 'iso_ha', 'snap_m', 'near_m', 'near',
            'island_nodes', 'island_private_doors', 'island_gate_doors', 'stop_lat', 'stop_lon']
    stops[cols].to_csv(out_dir / 'stops_access.csv', index=False)

    print(f"\n{len(stops):,} stops, {stops['trip_count'].sum():,} departures")
    for flag in ('SKIPPED', 'SMALL_ISO', 'SNAP_FAR', 'IN_GATED_ISLAND', 'IN_LOOSE_ISLAND'):
        sub = stops[stops['flags'].str.contains(flag)]
        if len(sub):
            print(f"  {flag:16s} {len(sub):5,} stops, {sub['trip_count'].sum():8,} departures "
                  f"({sub['trip_count'].sum() / stops['trip_count'].sum() * 100:.1f} %)")
    worst = stops[stops['flags'].str.contains('SKIPPED|SNAP_FAR|IN_GATED')].sort_values('trip_count', ascending=False)
    print("\nBusiest flagged stops (see diagnostics/stops_access.csv):")
    print(worst[['stop_name', 'trip_count', 'flags', 'iso_ha', 'snap_m', 'island_private_doors',
                 'island_gate_doors']].head(20).to_string(index=False))

    if args.stop:
        sel = stops[stops['stop_name'].str.contains(args.stop, case=False, na=False)]
        for name, grp in sel.groupby('stop_name'):
            sketch(city, data, name, grp, coords, r, c, pub, node_island, gate_net, isos, sxy,
                   snap_idx, stops, args.radius)


def sketch(city, data, name, grp, coords, r, c, pub, node_island, gate_net, isos, sxy, snap_idx,
           stops, radius):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.collections import LineCollection
    ii = grp.index.to_numpy()
    cx, cy = sxy[ii].mean(axis=0)
    x0, x1, y0, y1 = cx - radius, cx + radius, cy - radius, cy + radius
    inwin = lambda xy: (xy[:, 0] > x0) & (xy[:, 0] < x1) & (xy[:, 1] > y0) & (xy[:, 1] < y1)
    m = inwin(coords[r]) | inwin(coords[c])
    segs = np.stack([coords[r[m]], coords[c[m]]], axis=1)
    is_isl = (node_island[r[m]] >= 0) & (node_island[c[m]] >= 0) & pub[m]
    is_priv = ~pub[m]
    fig, ax = plt.subplots(figsize=(9, 9))
    bld = gpd.read_file(city['osm_dir'] / 'buildings.gpkg', bbox=tuple(
        gpd.GeoSeries.from_xy([x0, x1], [y0, y1], crs=city['crs_metric']).to_crs(4326).total_bounds))
    if len(bld):
        bld.to_crs(city['crs_metric']).plot(ax=ax, color='#dddddd', edgecolor='#999999', lw=0.4)
    sub = isos[isos['stop_id'].astype(str).isin(grp['stop_id'])]
    if len(sub):
        sub.boundary.plot(ax=ax, color='#c2185b', lw=1.5)
    for mask, col, lw in ((~is_isl & ~is_priv, '#888888', 0.6), (is_priv, '#ff9800', 1.0), (is_isl, '#00bcd4', 1.6)):
        ax.add_collection(LineCollection(segs[mask], colors=col, linewidths=lw))
    for i in ii:
        ax.plot(*sxy[i], marker='*', ms=16, color='blue', zorder=5)
        ax.plot([sxy[i][0], coords[snap_idx[i]][0]], [sxy[i][1], coords[snap_idx[i]][1]],
                'r--', lw=1.5, zorder=4)
        ax.plot(*coords[snap_idx[i]], 'ro', ms=6, zorder=5)
    row = grp.iloc[0]
    ax.set_xlim(x0, x1)
    ax.set_ylim(y0, y1)
    ax.set_aspect('equal')
    ax.set_axis_off()
    ax.set_title(f"{name} ({city['name']}): {row['flags'] or 'ok'}\n"
                 f"walk starts {row.snap_m:.0f} m away (red), isochrone {row.iso_ha:.1f} ha (magenta); "
                 f"island paths cyan ({row.island_nodes} nodes, {row.island_private_doors} private-way "
                 f"+ {row.island_gate_doors} gate doors), private orange", fontsize=9)
    out = HERE.parent / 'png' / 'previews' / f"diag_{city['key']}_{re.sub(r'[^a-z0-9]+', '_', name.lower()).strip('_')}.png"
    fig.savefig(out, dpi=110, bbox_inches='tight')
    plt.close(fig)
    print(f"Saved {out}")


if __name__ == '__main__':
    main()
