"""
Buildings and building:parts with their height tags, read straight from an OSM .pbf (pyosmium),
for basemaps that kept only osm_id + building (the Geofabrik route, e.g. New York).

    python 3d/osm_buildings_3d.py --pbf D:/QGIS/osm_basemap/pbf/newyork_new-york.osm.pbf \
        --out D:/QGIS/osm_basemap/newyork/buildings_3d.gpkg

Columns match the Overpass basemaps (osm_id = way id, or relation id for multipolygons, as
osm_local.py / Overpass number them): osm_id, building, building_part, height, min_height,
building_levels, building_min_level, roof_shape, name. Underground structures are skipped.
export_scene.py --buildings uses the file.
"""
import argparse
from pathlib import Path

import geopandas as gpd
import osmium
import shapely.wkb

TAGS = {'building': 'building', 'building:part': 'building_part', 'height': 'height',
        'min_height': 'min_height', 'building:levels': 'building_levels',
        'building:min_level': 'building_min_level', 'roof:shape': 'roof_shape', 'name': 'name'}


class Buildings(osmium.SimpleHandler):
    def __init__(self):
        super().__init__()
        self.wkb = osmium.geom.WKBFactory()
        self.rows, self.geoms, self.failed = [], [], 0

    def area(self, a):
        t = a.tags
        if not ('building' in t or 'building:part' in t):
            return
        if t.get('location') == 'underground' or t.get('building') == 'no':
            return
        try:
            g = shapely.wkb.loads(self.wkb.create_multipolygon(a), hex=True)
        except Exception:  # broken multipolygons
            self.failed += 1
            return
        row = {col: t.get(k) for k, col in TAGS.items()}
        row['osm_id'] = a.orig_id()
        row['osm_type'] = 'way' if a.from_way() else 'relation'
        self.rows.append(row)
        self.geoms.append(g.geoms[0] if len(g.geoms) == 1 else g)


def main():
    ap = argparse.ArgumentParser(description=__doc__.split('\n\n')[0])
    ap.add_argument('--pbf', required=True)
    ap.add_argument('--out', required=True)
    args = ap.parse_args()
    h = Buildings()
    h.apply_file(args.pbf, locations=True)
    gdf = gpd.GeoDataFrame(h.rows, geometry=h.geoms, crs='EPSG:4326')
    out = Path(args.out)
    gdf.to_file(out, driver='GPKG', layer='buildings')
    print(f"{out}: {len(gdf):,} buildings/parts ({gdf['building_part'].notna().sum():,} parts, "
          f"height {gdf['height'].notna().mean():.0%}, levels {gdf['building_levels'].notna().mean():.0%}; "
          f"{h.failed} broken multipolygons skipped)")


if __name__ == '__main__':
    main()
