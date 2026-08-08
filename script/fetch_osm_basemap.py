"""
Fetch OSM basemap data for any city.
Downloads water, parks, forests, roads, buildings, railways, etc. via Overpass API.

Usage:
    python fetch_osm_basemap.py --city poznan
    python fetch_osm_basemap.py --city warsaw
    python fetch_osm_basemap.py --city berlin --skip-buildings

Buildings are fetched via OSMnx (tiled) by default. Use --skip-buildings
if you already have building data or want to fetch them separately.
"""
import argparse
import time as time_mod
import requests
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point, LineString, Polygon, MultiPolygon, box
from pathlib import Path
from datetime import datetime

from cities import get_city, add_city_argument

OVERPASS_URL = "https://overpass.kumi.systems/api/interpreter"


# ---------------------------------------------------------------------------
# Overpass query templates (bbox placeholder: {bbox})
# All generic -- no city-specific filters.
# ---------------------------------------------------------------------------

SIMPLE_QUERIES = {
    'water': """
[out:json][timeout:120];
(
  way["natural"="water"]({bbox});
  relation["natural"="water"]({bbox});
);
out geom;
""",
    'waterways': """
[out:json][timeout:120];
(
  way["waterway"="river"]({bbox});
  way["waterway"="stream"]({bbox});
  way["waterway"="canal"]({bbox});
);
out geom;
""",
    'parks': """
[out:json][timeout:120];
(
  way["leisure"="park"]({bbox});
  relation["leisure"="park"]({bbox});
);
out geom;
""",
    'leisure': """
[out:json][timeout:120];
(
  way["leisure"="garden"]({bbox});
  way["leisure"="playground"]({bbox});
  way["leisure"="pitch"]({bbox});
  way["leisure"="sports_centre"]({bbox});
  way["leisure"="stadium"]({bbox});
  way["leisure"="track"]({bbox});
  way["leisure"="swimming_pool"]({bbox});
  way["leisure"="recreation_ground"]({bbox});
  way["leisure"="dog_park"]({bbox});
  way["leisure"="fitness_station"]({bbox});
  way["leisure"="golf_course"]({bbox});
);
out geom;
""",
    'leisure_relations': """
[out:json][timeout:120];
(
  relation["leisure"="garden"]({bbox});
  relation["leisure"="playground"]({bbox});
  relation["leisure"="pitch"]({bbox});
  relation["leisure"="sports_centre"]({bbox});
  relation["leisure"="stadium"]({bbox});
  relation["leisure"="golf_course"]({bbox});
);
out geom;
""",
    'forests': """
[out:json][timeout:120];
(
  way["landuse"="forest"]({bbox});
  relation["landuse"="forest"]({bbox});
  way["natural"="wood"]({bbox});
  relation["natural"="wood"]({bbox});
);
out geom;
""",
    'grass': """
[out:json][timeout:120];
(
  way["landuse"="grass"]({bbox});
  relation["landuse"="grass"]({bbox});
);
out geom;
""",
    'meadow': """
[out:json][timeout:120];
(
  way["landuse"="meadow"]({bbox});
  relation["landuse"="meadow"]({bbox});
);
out geom;
""",
    'allotments': """
[out:json][timeout:120];
(
  way["landuse"="allotments"]({bbox});
  relation["landuse"="allotments"]({bbox});
);
out geom;
""",
    'cemeteries': """
[out:json][timeout:120];
(
  way["landuse"="cemetery"]({bbox});
  way["amenity"="grave_yard"]({bbox});
  relation["landuse"="cemetery"]({bbox});
  relation["amenity"="grave_yard"]({bbox});
);
out geom;
""",
    'railways': """
[out:json][timeout:120];
(
  way["railway"="rail"]({bbox});
  way["railway"="light_rail"]({bbox});
  way["railway"="subway"]({bbox});
  way["railway"="tram"]({bbox});
);
out geom;
""",
}

TILED_QUERIES = {
    'roads': """
[out:json][timeout:90];
(
  way["highway"="motorway"]({bbox});
  way["highway"="trunk"]({bbox});
  way["highway"="primary"]({bbox});
  way["highway"="secondary"]({bbox});
  way["highway"="tertiary"]({bbox});
  way["highway"="unclassified"]({bbox});
  way["highway"="residential"]({bbox});
  way["highway"="living_street"]({bbox});
  way["highway"="motorway_link"]({bbox});
  way["highway"="trunk_link"]({bbox});
  way["highway"="primary_link"]({bbox});
  way["highway"="secondary_link"]({bbox});
  way["highway"="tertiary_link"]({bbox});
);
out geom;
""",
}

# Column filters
COLUMN_FILTERS = {
    'railways': ['railway', 'name', 'usage', 'service', 'bridge', 'tunnel', 'layer', 'gauge', 'electrified'],
    'roads': ['highway', 'name', 'lanes', 'surface', 'bridge', 'tunnel', 'layer'],
}


# ---------------------------------------------------------------------------
# Geometry parsing (from your existing fetch_warsaw_osm.py)
# ---------------------------------------------------------------------------

def join_ways(ways):
    """Join multiple way segments into closed rings."""
    if not ways:
        return []
    if len(ways) == 1:
        coords = ways[0]
        if coords[0] == coords[-1]:
            return [coords]
        coords_copy = list(coords)
        coords_copy.append(coords_copy[0])
        return [coords_copy]

    way_segments = [c for c in ways if len(c) >= 2]
    if not way_segments:
        return []

    rings = []
    used = set()

    for start_idx in range(len(way_segments)):
        if start_idx in used:
            continue
        ring = list(way_segments[start_idx])
        used.add(start_idx)
        current_end = ring[-1]

        for _ in range(len(way_segments) * 2):
            dist_to_start = ((ring[0][0] - ring[-1][0])**2 + (ring[0][1] - ring[-1][1])**2)**0.5
            if dist_to_start < 0.00001:
                if ring[0] != ring[-1]:
                    ring.append(ring[0])
                rings.append(ring)
                break

            best_idx = None
            best_dist = float('inf')
            reverse = False

            for idx in range(len(way_segments)):
                if idx in used:
                    continue
                segment = way_segments[idx]
                d_start = ((segment[0][0] - current_end[0])**2 + (segment[0][1] - current_end[1])**2)**0.5
                d_end = ((segment[-1][0] - current_end[0])**2 + (segment[-1][1] - current_end[1])**2)**0.5
                if d_start < best_dist:
                    best_dist, best_idx, reverse = d_start, idx, False
                if d_end < best_dist:
                    best_dist, best_idx, reverse = d_end, idx, True

            if best_idx is not None:
                next_seg = way_segments[best_idx]
                if reverse:
                    next_seg = list(reversed(next_seg))
                if best_dist < 0.00001:
                    ring.extend(next_seg[1:])
                else:
                    ring.extend(next_seg)
                current_end = ring[-1]
                used.add(best_idx)
            else:
                if ring[0] != ring[-1]:
                    ring.append(ring[0])
                rings.append(ring)
                break
        else:
            if ring[0] != ring[-1]:
                ring.append(ring[0])
            if len(ring) >= 4:
                rings.append(ring)

    return rings


def parse_geometry(element):
    """Convert OSM element to Shapely geometry."""
    if element['type'] == 'node' and 'lat' in element and 'lon' in element:
        return Point(element['lon'], element['lat'])

    if element['type'] == 'way' and 'geometry' in element:
        coords = [(n['lon'], n['lat']) for n in element['geometry']]
        if len(coords) > 1:
            if coords[0] == coords[-1] and len(coords) > 3:
                return Polygon(coords)
            return LineString(coords)

    if element['type'] == 'relation' and 'members' in element:
        outer_ways, inner_ways, line_members = [], [], []
        for member in element['members']:
            if 'geometry' in member and member['type'] == 'way':
                coords = [(n['lon'], n['lat']) for n in member['geometry']]
                if len(coords) > 1:
                    role = member.get('role', '')
                    if role == 'outer':
                        outer_ways.append(coords)
                    elif role == 'inner':
                        inner_ways.append(coords)
                    else:
                        line_members.append(coords)

        if outer_ways:
            try:
                outer_rings = join_ways(outer_ways)
                inner_rings = join_ways(inner_ways) if inner_ways else []
                if outer_rings:
                    if len(outer_rings) == 1:
                        return Polygon(outer_rings[0], inner_rings) if inner_rings else Polygon(outer_rings[0])
                    polygons = []
                    for outer_ring in outer_rings:
                        try:
                            if inner_rings:
                                outer_poly = Polygon(outer_ring)
                                holes = [ir for ir in inner_rings
                                         if outer_poly.contains(Polygon(ir))]
                                polygons.append(Polygon(outer_ring, holes) if holes else Polygon(outer_ring))
                            else:
                                polygons.append(Polygon(outer_ring))
                        except Exception:
                            continue
                    if len(polygons) == 1:
                        return polygons[0]
                    if polygons:
                        return MultiPolygon(polygons)
            except Exception:
                pass
        elif line_members:
            try:
                from shapely.geometry import MultiLineString
                lines = [LineString(c) for c in line_members if len(c) > 1]
                if lines:
                    return MultiLineString(lines)
            except Exception:
                pass

    return None


# ---------------------------------------------------------------------------
# Query execution
# ---------------------------------------------------------------------------

def query_overpass(query, layer_name, tile_id="", filter_columns=None):
    """Execute Overpass query with retry."""
    for attempt in range(3):
        try:
            if attempt > 0:
                print(f"retry {attempt}...", end=' ', flush=True)
            response = requests.post(OVERPASS_URL, data={'data': query}, timeout=180)
            if response.status_code != 200:
                print(f"HTTP {response.status_code}...", end=' ')
                if attempt < 2:
                    time_mod.sleep(30)
                    continue
                return None

            try:
                data = response.json()
            except ValueError:
                print(f"invalid JSON...", end=' ')
                if attempt < 2:
                    time_mod.sleep(30)
                    continue
                return None

            geometries, properties_list = [], []
            for element in data.get('elements', []):
                geom = parse_geometry(element)
                if geom is None:
                    continue
                tags = element.get('tags', {})
                if filter_columns:
                    props = {col: tags.get(col) for col in filter_columns}
                else:
                    props = dict(tags)
                props['osm_id'] = element['id']
                props['osm_type'] = element['type']
                geometries.append(geom)
                properties_list.append(props)

            if geometries:
                return gpd.GeoDataFrame(properties_list, geometry=geometries, crs="EPSG:4326")
            return None

        except requests.exceptions.Timeout:
            if attempt < 2:
                wait = 20 * (attempt + 1)
                print(f"timeout, wait {wait}s...", end=' ', flush=True)
                time_mod.sleep(wait)
        except Exception as e:
            print(f"error: {str(e)[:50]}...", end=' ')
            if attempt < 2:
                time_mod.sleep(15)
    return None


def create_tiles(bbox, n_tiles=4):
    """Split bbox into n_tiles x n_tiles grid."""
    lat_step = (bbox['north'] - bbox['south']) / n_tiles
    lon_step = (bbox['east'] - bbox['west']) / n_tiles
    tiles = []
    for i in range(n_tiles):
        for j in range(n_tiles):
            tiles.append({
                'south': bbox['south'] + i * lat_step,
                'north': bbox['south'] + (i + 1) * lat_step,
                'west': bbox['west'] + j * lon_step,
                'east': bbox['west'] + (j + 1) * lon_step,
                'id': f"{i}_{j}",
            })
    return tiles


def fetch_tiled(layer_name, query_template, tiles, filter_columns=None):
    """Fetch layer across tiles, merge, deduplicate."""
    print(f"{layer_name} (tiled)...", end=' ', flush=True)
    all_gdfs = []

    for i, tile in enumerate(tiles):
        bbox_str = f"{tile['south']},{tile['west']},{tile['north']},{tile['east']}"
        query = query_template.format(bbox=bbox_str)
        if i > 0:
            print(f"{i+1}/{len(tiles)}...", end=' ', flush=True)
        gdf = query_overpass(query, layer_name, tile['id'], filter_columns=filter_columns)
        if gdf is not None and not gdf.empty:
            all_gdfs.append(gdf)
        if i < len(tiles) - 1:
            time_mod.sleep(4)

    if all_gdfs:
        merged = gpd.GeoDataFrame(pd.concat(all_gdfs, ignore_index=True),
                                  geometry='geometry', crs="EPSG:4326")
        if 'osm_id' in merged.columns:
            before = len(merged)
            merged = merged.drop_duplicates(subset=['osm_id'], keep='first')
            print(f"dedup {before}->{len(merged)}...", end=' ')
        merged = merged[merged.geometry.notnull()]
        return merged if len(merged) > 0 else None
    return None


# ---------------------------------------------------------------------------
# Buildings via OSMnx (separate, tiled approach)
# ---------------------------------------------------------------------------

def fetch_buildings_osmnx(bbox, output_file, crs_metric, n_tiles=4):
    """Fetch buildings via OSMnx, tiled for reliability."""
    import osmnx as ox

    ox.settings.max_query_area_size = 10_000_000
    ox.settings.requests_pause = 2

    tiles_dir = output_file.parent / "building_tiles"
    tiles_dir.mkdir(parents=True, exist_ok=True)

    tiles = create_tiles(bbox, n_tiles)
    done = []

    for i, tile in enumerate(tiles, 1):
        tile_file = tiles_dir / f"tile_{tile['id']}.gpkg"
        if tile_file.exists():
            print(f"  [{i}/{len(tiles)}] {tile['id']} -- cached")
            done.append(tile_file)
            continue

        print(f"  [{i}/{len(tiles)}] {tile['id']}...", end=" ", flush=True)
        polygon = box(tile['west'], tile['south'], tile['east'], tile['north'])
        try:
            gdf = ox.features_from_polygon(polygon, tags={"building": True})
            gdf = gdf.reset_index()

            # Normalize columns
            col_map = {'osmid': 'osm_id', 'element_type': 'osm_type',
                       'building:levels': 'levels', 'addr:housenumber': 'housenumber',
                       'addr:street': 'street'}
            gdf = gdf.rename(columns={k: v for k, v in col_map.items() if k in gdf.columns})

            keep = ['osm_id', 'osm_type', 'name', 'building', 'levels',
                    'housenumber', 'street', 'geometry']
            for c in keep:
                if c not in gdf.columns:
                    gdf[c] = None
            gdf = gdf[keep]
            gdf = gdf[gdf.geometry.geom_type.isin(['Polygon', 'MultiPolygon'])].copy()
            gdf['levels'] = pd.to_numeric(gdf['levels'], errors='coerce').fillna(2)

            # Simplify
            gdf = gdf.to_crs(crs_metric)
            gdf['geometry'] = gdf.geometry.simplify(tolerance=5, preserve_topology=True)
            gdf = gdf.to_crs("EPSG:4326")

            if len(gdf) > 0:
                gdf.to_file(tile_file, driver="GPKG")
                print(f"{len(gdf):,} buildings")
                done.append(tile_file)
            else:
                print("no buildings")
        except Exception as e:
            print(f"FAILED: {e}")

    if not done:
        print("No building tiles fetched!")
        return

    print(f"\nMerging {len(done)} tiles...", end=" ", flush=True)
    merged = gpd.GeoDataFrame(
        pd.concat([gpd.read_file(f) for f in done], ignore_index=True),
        geometry="geometry", crs="EPSG:4326"
    )
    before = len(merged)
    if 'osm_id' in merged.columns:
        merged = merged.drop_duplicates(subset=["osm_id"], keep="first")
    merged = merged[merged.geometry.notnull()]
    print(f"{before:,} -> {len(merged):,} after dedup")

    merged.to_file(output_file, driver="GPKG")
    print(f"Saved {len(merged):,} buildings to {output_file.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description='Fetch OSM basemap data for a city')
    add_city_argument(parser)
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory (default: basemap/<city>/)')
    parser.add_argument('--skip-buildings', action='store_true',
                        help='Skip building download (slow)')
    parser.add_argument('--only', type=str, default=None,
                        help='Only fetch this layer (e.g. --only water)')
    args = parser.parse_args()

    city = get_city(args.city)
    bbox = city['bbox']
    crs_metric = city['crs_metric']
    output_dir = Path(args.output) if args.output else city['osm_dir']
    output_dir.mkdir(parents=True, exist_ok=True)

    bbox_str = f"{bbox['south']},{bbox['west']},{bbox['north']},{bbox['east']}"

    print("=" * 60)
    print(f"OSM Basemap Fetcher -- {city['name']}")
    print("=" * 60)
    print(f"Bbox: {bbox_str}")
    print(f"Output: {output_dir}")
    print(f"Server: {OVERPASS_URL}\n")

    start_time = datetime.now()
    success, skipped = 0, 0

    # Determine which layers to fetch
    all_layers = list(SIMPLE_QUERIES.keys()) + list(TILED_QUERIES.keys())
    if args.only:
        if args.only not in SIMPLE_QUERIES and args.only not in TILED_QUERIES and args.only != 'buildings':
            print(f"Unknown layer: {args.only}. Available: {', '.join(all_layers + ['buildings'])}")
            return

    total = len(SIMPLE_QUERIES) + len(TILED_QUERIES) + (0 if args.skip_buildings else 1)

    # Simple queries
    for i, (layer_name, query_template) in enumerate(SIMPLE_QUERIES.items(), 1):
        if args.only and args.only != layer_name:
            continue

        output_file = output_dir / f"{layer_name}.gpkg"
        if output_file.exists():
            print(f"[{i}/{total}] {layer_name}... Already exists, skipping")
            skipped += 1
            continue

        print(f"[{i}/{total}] {layer_name}...", end=' ', flush=True)
        filter_cols = COLUMN_FILTERS.get(layer_name)

        try:
            query = query_template.format(bbox=bbox_str)
            gdf = query_overpass(query, layer_name, filter_columns=filter_cols)
            if gdf is not None and not gdf.empty:
                # Drop osm_id for admin boundaries to avoid FID issues
                if 'admin' in layer_name:
                    gdf = gdf.drop(columns=[c for c in ['osm_id', 'osm_type'] if c in gdf.columns])
                    gdf = gdf.reset_index(drop=True)
                gdf.to_file(output_file, driver="GPKG")
                print(f"OK {len(gdf)} features -> {output_file.name}")
                success += 1
            else:
                print("No data")
        except Exception as e:
            print(f"FAILED: {e}")

        time_mod.sleep(3)

    # Tiled queries (roads)
    for layer_name, query_template in TILED_QUERIES.items():
        if args.only and args.only != layer_name:
            continue

        output_file = output_dir / f"{layer_name}.gpkg"
        if output_file.exists():
            print(f"{layer_name}... Already exists, skipping")
            skipped += 1
            continue

        tiles = create_tiles(bbox, n_tiles=4)
        filter_cols = COLUMN_FILTERS.get(layer_name)

        try:
            gdf = fetch_tiled(layer_name, query_template, tiles, filter_columns=filter_cols)
            if gdf is not None and not gdf.empty:
                # Add hierarchy for roads
                if layer_name == 'roads' and 'highway' in gdf.columns:
                    hierarchy_map = {
                        'motorway': 1, 'trunk': 2, 'primary': 3, 'secondary': 4,
                        'tertiary': 5, 'unclassified': 6, 'residential': 7,
                        'living_street': 8, 'motorway_link': 1, 'trunk_link': 2,
                        'primary_link': 3, 'secondary_link': 4, 'tertiary_link': 5,
                    }
                    gdf['hierarchy'] = gdf['highway'].map(hierarchy_map).fillna(99)

                gdf.to_file(output_file, driver="GPKG")
                print(f"OK {len(gdf)} features -> {output_file.name}")
                success += 1
            else:
                print("No data")
        except Exception as e:
            print(f"FAILED: {e}")

        time_mod.sleep(3)

    # Buildings (OSMnx)
    if not args.skip_buildings and (not args.only or args.only == 'buildings'):
        buildings_file = output_dir / "buildings.gpkg"
        if buildings_file.exists():
            print(f"\nBuildings... Already exists, skipping")
            skipped += 1
        else:
            print(f"\nFetching buildings via OSMnx...")
            try:
                fetch_buildings_osmnx(bbox, buildings_file, crs_metric, n_tiles=4)
                success += 1
            except Exception as e:
                print(f"Buildings FAILED: {e}")

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\nDone! {success} new, {skipped} skipped in {elapsed/60:.1f} min")
    print(f"Output: {output_dir}")
    print(f"\nTo re-fetch a layer, delete its .gpkg file from {output_dir}")


if __name__ == "__main__":
    main()
