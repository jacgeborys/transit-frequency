"""
Local stand-in for the Overpass API: answers the fetchers' queries from OSM extracts
(.osm.pbf, e.g. from Geofabrik) instead of the public server.

overpass_polite.post() routes here when the environment variable OSM_LOCAL_PBF is set
(';'-separated city extracts made by make_osm_extract.py), so the basemap fetcher, 02 and
drop_underground_buildings.py work unchanged and return the same JSON as Overpass.

Supported query language (all the fetchers use):
  [out:json][timeout:N];
  ( <stmt>; <stmt>; ... [>;] );   or a single statement without the union
  out; | out geom; | out tags;
  <stmt> = node|way|relation, filters ["k"] [!"k"] ["k"="v"] ["k"!="v"] ["k"~"re"] ["k"!~"re"],
           then (south,west,north,east)
Semantics as on Overpass: ~ is an unanchored, case-sensitive regex; != and !~ also match
elements without the key; a way is in the bbox if one of its nodes or segments is, a
relation if one of its member ways/nodes is; `>;` adds the nodes of the selected ways
(and the member ways + nodes of relations). Ways whose nodes are missing from the extract
lose those nodes (make_osm_extract.py completes every way and relation it keeps).

A copy of this file lives next to D:\\QGIS\\osm_basemap\\fetch_osm_basemap.py (not a git
repo); keep the two in sync.
"""
import re
import time

import numpy as np
import osmium
from shapely.geometry import LineString, box

_STORE = {}   # files key -> _Store (one per process)

_STMT = re.compile(r'(node|way|relation)((?:\[[^\]]*\])*)\(\s*([-\d.]+)\s*,\s*([-\d.]+)\s*,'
                   r'\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\)\s*;')
_FILTER = re.compile(r'\[\s*(!?)"([^"]+)"\s*(?:(=|!=|~|!~)\s*"([^"]*)")?\s*\]')
_OUT = re.compile(r'out\s*(geom|tags)?\s*;')


class _Store:
    """Everything in the extracts, with per-element bounding boxes for fast bbox queries."""

    def __init__(self, files):
        t0 = time.time()
        n_ids, n_lon, n_lat, n_tags = [], [], [], []
        self.ways = {}   # id -> (tags, refs int64[], coords float64[n, 2] lon/lat)
        self.rels = {}   # id -> (tags, [(type 'n'|'w'|'r', ref, role)])
        seen_nodes = set()
        for f in files:
            for o in osmium.FileProcessor(str(f)).with_locations():
                if o.is_node():
                    if len(o.tags) and o.id not in seen_nodes and o.location.valid():
                        seen_nodes.add(o.id)
                        n_ids.append(o.id)
                        n_lon.append(o.location.lon)
                        n_lat.append(o.location.lat)
                        n_tags.append(dict(o.tags))
                elif o.is_way():
                    if o.id in self.ways:
                        continue
                    refs, coords = [], []
                    for n in o.nodes:
                        if n.location.valid():
                            refs.append(n.ref)
                            coords.append((n.location.lon, n.location.lat))
                    if refs:
                        self.ways[o.id] = (dict(o.tags), np.array(refs, dtype=np.int64),
                                           np.array(coords, dtype=np.float64))
                elif o.is_relation():
                    if o.id not in self.rels:
                        self.rels[o.id] = (dict(o.tags),
                                           [(m.type, m.ref, m.role) for m in o.members])
        self.n_ids = np.array(n_ids, dtype=np.int64)
        self.n_lon, self.n_lat = np.array(n_lon), np.array(n_lat)
        self.n_tags = n_tags
        self.node_pos = {nid: i for i, nid in enumerate(n_ids)}

        self.w_ids = np.fromiter(self.ways.keys(), dtype=np.int64, count=len(self.ways))
        bb = np.array([(c[:, 0].min(), c[:, 1].min(), c[:, 0].max(), c[:, 1].max())
                       for _, _, c in self.ways.values()]).reshape(-1, 4)
        self.w_bb = bb
        way_bb = dict(zip(self.w_ids.tolist(), map(tuple, bb)))
        r_ids, r_bb = [], []
        for rid, (_, members) in self.rels.items():
            boxes = [way_bb[ref] for t, ref, _ in members if t == 'w' and ref in way_bb]
            boxes += [(self.n_lon[self.node_pos[ref]], self.n_lat[self.node_pos[ref]]) * 2
                      for t, ref, _ in members if t == 'n' and ref in self.node_pos]
            if boxes:
                a = np.array(boxes)
                r_ids.append(rid)
                r_bb.append((a[:, 0].min(), a[:, 1].min(), a[:, 2].max(), a[:, 3].max()))
        self.r_ids = np.array(r_ids, dtype=np.int64)
        self.r_bb = np.array(r_bb).reshape(-1, 4)
        print(f"[osm_local: {len(n_ids):,} tagged nodes, {len(self.ways):,} ways, "
              f"{len(self.rels):,} relations loaded in {time.time() - t0:.0f}s]", end=' ', flush=True)

    # -- spatial tests -------------------------------------------------------------------
    @staticmethod
    def _overlaps(bb, q):
        w, s, e, n = q
        return (bb[:, 0] <= e) & (bb[:, 2] >= w) & (bb[:, 1] <= n) & (bb[:, 3] >= s)

    def way_in(self, wid, q):
        w, s, e, n = q
        c = self.ways[wid][2]
        if np.any((c[:, 0] >= w) & (c[:, 0] <= e) & (c[:, 1] >= s) & (c[:, 1] <= n)):
            return True
        return len(c) > 1 and LineString(c).intersects(box(w, s, e, n))

    def node_in(self, nid, q):
        i = self.node_pos.get(nid)
        if i is None:
            return False
        w, s, e, n = q
        return w <= self.n_lon[i] <= e and s <= self.n_lat[i] <= n

    def rel_in(self, rid, q):
        for t, ref, _ in self.rels[rid][1]:
            if t == 'w' and ref in self.ways and self.way_in(ref, q):
                return True
            if t == 'n' and self.node_in(ref, q):
                return True
        return False

    # -- selection -----------------------------------------------------------------------
    def select(self, etype, filters, q):
        if etype == 'node':
            w, s, e, n = q
            idx = np.nonzero((self.n_lon >= w) & (self.n_lon <= e) &
                             (self.n_lat >= s) & (self.n_lat <= n))[0]
            return [int(self.n_ids[i]) for i in idx if _match(self.n_tags[i], filters)]
        if etype == 'way':
            cand = self.w_ids[self._overlaps(self.w_bb, q)]
            return [int(i) for i in cand
                    if _match(self.ways[int(i)][0], filters) and self.way_in(int(i), q)]
        cand = self.r_ids[self._overlaps(self.r_bb, q)]
        return [int(i) for i in cand
                if _match(self.rels[int(i)][0], filters) and self.rel_in(int(i), q)]

    # -- output --------------------------------------------------------------------------
    def node_json(self, nid, coords=None, tags=True):
        i = self.node_pos.get(nid)
        if coords is None:
            coords = (self.n_lon[i], self.n_lat[i])
        el = {'type': 'node', 'id': nid, 'lat': float(coords[1]), 'lon': float(coords[0])}
        if tags and i is not None:
            el['tags'] = self.n_tags[i]
        return el

    def way_geom(self, wid):
        return [{'lat': float(y), 'lon': float(x)} for x, y in self.ways[wid][2]]


def _match(tags, filters):
    for neg, k, op, v in filters:
        val = tags.get(k)
        if op is None:
            ok = (val is None) if neg else (val is not None)
        elif op == '=':
            ok = val == v
        elif op == '!=':
            ok = val != v
        elif op == '~':
            ok = val is not None and re.search(v, val) is not None
        else:  # !~
            ok = val is None or re.search(v, val) is None
        if not ok:
            return False
    return True


def _store(files):
    key = tuple(str(f) for f in files)
    if key not in _STORE:
        _STORE.clear()
        _STORE[key] = _Store(files)
    return _STORE[key]


def query(ql, files):
    """Answer an Overpass QL query (subset, see module doc) from the extracts, as Overpass JSON."""
    st = _store(files)
    body = ql.split(';', 1)[1] if ql.lstrip().startswith('[') else ql  # drop [out:json][timeout]
    stmts = _STMT.findall(body)
    if not stmts:
        raise ValueError(f"osm_local: no statement understood in query: {ql[:200]}")
    recurse = re.search(r'>\s*;', body) is not None
    m = _OUT.search(body)
    mode = (m.group(1) or 'body') if m else 'body'

    sel = {'node': set(), 'way': set(), 'relation': set()}
    for etype, fstr, s, w, n, e in stmts:
        filters = [(neg == '!', k, op or None, v) for neg, k, op, v in _FILTER.findall(fstr)]
        q = (float(w), float(s), float(e), float(n))
        sel[etype].update(st.select(etype, filters, q))

    extra_nodes = {}   # node id -> (lon, lat) of untagged way nodes added by >;
    if recurse:
        for rid in sel['relation']:
            for t, ref, _ in st.rels[rid][1]:
                if t == 'w' and ref in st.ways:
                    sel['way'].add(ref)
                elif t == 'n' and ref in st.node_pos:
                    sel['node'].add(ref)
        for wid in sel['way']:
            _, refs, coords = st.ways[wid]
            for r, c in zip(refs.tolist(), coords):
                if r in st.node_pos:
                    sel['node'].add(r)
                else:
                    extra_nodes[r] = c

    elements = []
    for nid in sorted(sel['node']):
        el = st.node_json(nid)
        if mode == 'tags':
            el = {'type': 'node', 'id': nid, 'tags': el.get('tags', {})}
        elements.append(el)
    for nid in sorted(extra_nodes):
        elements.append(st.node_json(nid, coords=extra_nodes[nid], tags=False))
    for wid in sorted(sel['way']):
        tags, refs, _ = st.ways[wid]
        el = {'type': 'way', 'id': wid, 'tags': tags}
        if mode != 'tags':
            el['nodes'] = refs.tolist()
        if mode == 'geom':
            el['geometry'] = st.way_geom(wid)
        elements.append(el)
    for rid in sorted(sel['relation']):
        tags, members = st.rels[rid]
        el = {'type': 'relation', 'id': rid, 'tags': tags}
        if mode != 'tags':
            ms = []
            for t, ref, role in members:
                mtype = {'n': 'node', 'w': 'way', 'r': 'relation'}[t]
                mem = {'type': mtype, 'ref': ref, 'role': role}
                if mode == 'geom':
                    if t == 'w' and ref in st.ways:
                        mem['geometry'] = st.way_geom(ref)
                    elif t == 'n' and ref in st.node_pos:
                        i = st.node_pos[ref]
                        mem['lat'], mem['lon'] = float(st.n_lat[i]), float(st.n_lon[i])
                ms.append(mem)
            el['members'] = ms
        elements.append(el)
    return {'version': 0.6, 'generator': 'osm_local (Geofabrik extract)', 'elements': elements}
