"""
Pedestrian access rules for gate-aware isochrones.

Decides which gates are closed to the public and which ways are private, from
<osm_dir>/gates.gpkg and <osm_dir>/private_ways.gpkg (fetch_osm_basemap.py
--only gates / --only private_ways). Closed gates and private ways are not
part of the public walking network; residents can still use them to reach the
public network (see 03_generate_isochrones_local.py --gates).

Gate rules (first match wins), validated on Warsaw OSM data 2026-10:
  open    foot = permissive / designated / public
          (foot = yes alone proves nothing on a gate: mappers use it for "pedestrian
          gate", e.g. on fenced campuses - the gate then follows the rules below)
  closed  foot = private / no / customers / ...
  closed  locked = yes
  open    access = yes / permissive / designated / public
  closed  access = private / no / customers / residents / destination / ...
          (beats opening_hours / locked=no: school gates open at drop-off hours
          are still access=customers)
  open    locked = no
  open    has opening_hours (public with hours; the map shows daytime)
  open    untagged, within 15 m of a park or cemetery
  closed  untagged anywhere else (estates, ROD allotments, yards)
Lift gates are not loaded at all (pedestrians walk around them), nor are
open-by-design barriers (entrance, kissing_gate, stile, chain, ...).

Way rules:
  private if foot is restrictive, or access is restrictive and foot doesn't open it
"""
import geopandas as gpd

OPEN_VALS = {'yes', 'permissive', 'designated', 'public'}
CLOSED_VALS = {'private', 'no', 'customers', 'residents', 'permit', 'destination',
               'staff', 'military', 'delivery', 'emergency', 'forestry',
               'private;delivery', 'agricultural'}
PUBLIC_CONTEXT_LAYERS = ('parks', 'cemeteries')  # untagged gates here are open
CONTEXT_BUFFER_M = 15


def _str(v):
    return v if isinstance(v, str) else None


def gate_is_closed(tags: dict, in_public_area: bool) -> bool:
    foot, access = _str(tags.get('foot')), _str(tags.get('access'))
    if foot in OPEN_VALS and foot != 'yes':
        return False
    if foot in CLOSED_VALS:
        return True
    if tags.get('locked') == 'yes':
        return True
    if access in OPEN_VALS:
        return False
    if access in CLOSED_VALS:
        return True
    if tags.get('locked') == 'no':
        return False
    if _str(tags.get('opening_hours')):
        return False
    return not in_public_area


def closed_gate_ids(osm_dir, crs_metric: str) -> set:
    """OSM node ids of gates closed to the public."""
    gates = gpd.read_file(osm_dir / 'gates.gpkg').to_crs(crs_metric)
    in_public = gates.index.to_series().map(lambda _: False)
    for name in PUBLIC_CONTEXT_LAYERS:
        f = osm_dir / f'{name}.gpkg'
        if not f.exists():
            continue
        poly = gpd.read_file(f).to_crs(crs_metric)
        poly = poly[poly.geom_type.isin(['Polygon', 'MultiPolygon'])]
        zone = gpd.GeoDataFrame(geometry=poly.buffer(CONTEXT_BUFFER_M), crs=crs_metric)
        hits = gpd.sjoin(gates[['geometry']], zone, predicate='within', how='inner').index
        in_public[in_public.index.isin(hits)] = True
    closed = {
        int(r.osm_id) for r, pub in zip(gates.itertuples(), in_public)
        if gate_is_closed(r._asdict(), pub)
    }
    return closed


def private_way_ids(osm_dir) -> set:
    """OSM way ids of highways closed to the public on foot."""
    ways = gpd.read_file(osm_dir / 'private_ways.gpkg', ignore_geometry=True)
    out = set()
    for r in ways.itertuples():
        foot, access = _str(getattr(r, 'foot', None)), _str(getattr(r, 'access', None))
        if foot in CLOSED_VALS or (access in CLOSED_VALS and foot not in OPEN_VALS):
            out.add(int(r.osm_id))
    return out
