"""
Create Coverage Map from Overlapping Transit Isochrones
Planar subdivision approach — counts unique transit lines per area.

For each polygon piece created by overlapping isochrones, collects the union
of route_ids from all covering isochrones and counts distinct routes.
This prevents double-counting the same bus line from nearby stops.

Usage:
    python 04_create_coverage_map.py [data_folder]

    Defaults: _data/2026_08_02
"""
import sys
import geopandas as gpd
import numpy as np
from pathlib import Path
from datetime import datetime
from shapely.ops import unary_union, polygonize
from shapely.geometry import MultiPolygon, Polygon
from shapely.validation import make_valid
import warnings
warnings.filterwarnings('ignore')

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent


def fix_geometry(geom):
    """Fix a geometry using make_valid + buffer(0)."""
    if geom is None or geom.is_empty:
        return None
    try:
        geom = make_valid(geom)
        geom = geom.buffer(0)
        geom = geom.buffer(0.1).buffer(-0.1)
        geom = make_valid(geom)
        if geom.is_empty or geom.area < 10:
            return None
        return geom
    except Exception:
        return None


def create_coverage_map(isochrones_gdf):
    """
    Planar subdivision:
    1. Fix all geometries
    2. Union all boundaries into a planar graph
    3. Polygonize into minimal pieces
    4. For each piece, collect route_ids from overlapping isochrones
    5. Count unique routes per piece
    6. Dissolve adjacent pieces with the same count
    """
    print(f"\nCreating coverage map from {len(isochrones_gdf)} isochrones...\n")

    original_crs = isochrones_gdf.crs
    gdf = isochrones_gdf.to_crs("EPSG:2180").copy()

    # Step 1: Fix geometries (keep route_ids attached)
    print("Step 1: Fixing geometries...")
    valid_mask = []
    for idx, geom in enumerate(gdf.geometry):
        if (idx + 1) % 1000 == 0:
            print(f"    {idx + 1}/{len(gdf)}...")
        fixed = fix_geometry(geom)
        if fixed is not None:
            gdf.at[gdf.index[idx], 'geometry'] = fixed
            valid_mask.append(True)
        else:
            valid_mask.append(False)

    gdf = gdf[valid_mask].copy()
    print(f"  {len(gdf)} geometries validated\n")

    # Step 2: Build planar graph from all boundary rings
    print("Step 2: Building planar graph...")
    all_rings = []
    for geom in gdf.geometry:
        if geom.geom_type == 'Polygon':
            all_rings.append(geom.exterior)
            all_rings.extend(geom.interiors)
        elif geom.geom_type == 'MultiPolygon':
            for poly in geom.geoms:
                all_rings.append(poly.exterior)
                all_rings.extend(poly.interiors)

    print(f"  {len(all_rings)} boundary rings")
    print(f"  Merging boundaries (this may take a while)...")
    planar_graph = unary_union(all_rings)

    # Step 3: Polygonize
    print("Step 3: Polygonizing...")
    pieces = list(polygonize(planar_graph))
    print(f"  {len(pieces)} minimal polygons\n")

    if not pieces:
        print("ERROR: No polygons created")
        return None

    # Step 4: Count unique routes per piece
    print("Step 4: Counting unique routes per piece...")
    pieces_gdf = gpd.GeoDataFrame(geometry=pieces, crs="EPSG:2180")

    # Use representative points to find which isochrones cover each piece
    rep_points = pieces_gdf.geometry.representative_point()
    rep_gdf = gpd.GeoDataFrame(
        {'piece_idx': pieces_gdf.index},
        geometry=rep_points,
        crs="EPSG:2180"
    )

    # Spatial join: for each representative point, find all isochrones it falls within
    iso_with_routes = gdf[['geometry', 'route_ids']].copy()
    iso_with_routes = iso_with_routes.reset_index(drop=True)

    joined = gpd.sjoin(rep_gdf, iso_with_routes, how='left', predicate='within')
    joined = joined.dropna(subset=['index_right'])

    # For each piece, collect all route_ids and count unique ones
    def count_unique_routes(group):
        all_routes = set()
        for route_str in group['route_ids'].dropna():
            if route_str:
                all_routes.update(route_str.split(','))
        return len(all_routes)

    unique_counts = joined.groupby('piece_idx').apply(count_unique_routes)
    pieces_gdf['unique_routes'] = pieces_gdf.index.map(unique_counts).fillna(0).astype(int)

    # Also count raw isochrone overlaps (total stops reachable)
    overlap_counts = joined.groupby('piece_idx').size()
    pieces_gdf['stops_reachable'] = pieces_gdf.index.map(overlap_counts).fillna(0).astype(int)

    pieces_gdf = pieces_gdf[pieces_gdf['unique_routes'] > 0].copy()
    print(f"  {len(pieces_gdf)} pieces with coverage\n")

    # Step 5: Dissolve adjacent pieces with the same unique_routes count
    print("Step 5: Dissolving adjacent pieces...")
    dissolved = pieces_gdf.dissolve(by='unique_routes', aggfunc={'stops_reachable': 'mean'}).reset_index()
    dissolved = dissolved.explode(index_parts=False).reset_index(drop=True)
    dissolved['stops_reachable'] = dissolved['stops_reachable'].round().astype(int)
    print(f"  {len(dissolved)} polygons after dissolve\n")

    # Step 6: Cleanup
    print("Step 6: Cleanup...")
    dissolved['geometry'] = dissolved.geometry.apply(
        lambda g: make_valid(g.buffer(0)) if g is not None and not g.is_empty else None
    )
    dissolved = dissolved[dissolved.geometry.notna()]
    dissolved = dissolved[~dissolved.geometry.is_empty]
    dissolved = dissolved[dissolved.geometry.is_valid]
    dissolved = dissolved[dissolved.geometry.area > 10]
    dissolved = dissolved[dissolved.geometry.area <= 5_000_000]

    dissolved['area_ha'] = dissolved.geometry.area / 10000
    dissolved = dissolved[['unique_routes', 'stops_reachable', 'area_ha', 'geometry']].copy()
    dissolved = dissolved.to_crs(original_crs)
    dissolved = dissolved.sort_values('unique_routes', ascending=False).reset_index(drop=True)

    print(f"  Final: {len(dissolved)} polygons\n")
    return dissolved


def main():
    default_data = PROJECT_DIR / "_data" / "2026_08_02"
    data_dir = Path(sys.argv[1]) if len(sys.argv) > 1 else default_data

    input_file = data_dir / "isochrones.gpkg"
    output_file = data_dir / "coverage_map.gpkg"

    print("=" * 60)
    print("Transit Coverage Map Generator")
    print("Planar subdivision — unique routes per area")
    print("=" * 60)
    print(f"Input:  {input_file}")
    print(f"Output: {output_file}\n")

    if not input_file.exists():
        print(f"Not found: {input_file}")
        print("Run 03_generate_isochrones_local.py first.")
        return

    print("Loading isochrones...", end=' ', flush=True)
    isochrones = gpd.read_file(input_file)
    print(f"{len(isochrones)} loaded")

    start_time = datetime.now()
    coverage = create_coverage_map(isochrones)

    if coverage is None or len(coverage) == 0:
        print("Failed to create coverage map!")
        return

    print("Saving...", end=' ', flush=True)
    coverage.to_file(output_file, driver="GPKG")
    elapsed = (datetime.now() - start_time).total_seconds()
    print("Done")

    print(f"\n{'='*60}")
    print(f"Coverage map created in {elapsed/60:.1f} min")
    print(f"{'='*60}")
    print(f"\n  Polygons: {len(coverage):,}")
    print(f"  Max unique routes: {coverage['unique_routes'].max()}")
    print(f"  Total coverage: {coverage['area_ha'].sum():.0f} ha")

    print(f"\nBreakdown:")
    for n in sorted(coverage['unique_routes'].unique(), reverse=True)[:15]:
        sub = coverage[coverage['unique_routes'] == n]
        print(f"  {n:>3} routes: {len(sub):>6,} polygons, {sub['area_ha'].sum():>8.1f} ha")

    print(f"\nSaved to: {output_file}")
    print(f"\nIn QGIS: style by 'unique_routes' column, graduated, Yellow-Orange-Red")


if __name__ == '__main__':
    main()
