import requests
import geopandas as gpd
from shapely.geometry import Polygon, mapping
import time
import os
import json
from shapely.ops import transform
import pyproj
from shapely.geometry import shape

def get_isochrone_data(api_key, latitude, longitude):
    time_limit = 300  # 5 minutes in seconds for walking distance
    url = f"https://graphhopper.com/api/1/isochrone?point={latitude},{longitude}&time_limit={time_limit}&vehicle=foot&key={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text), int(response.headers.get('X-RateLimit-Remaining', 0))
    else:
        return None, 0

def transform_geometry(geometry, src_crs, dst_crs):
    transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return transform(transformer.transform, geometry)

api_key = "e63ec932-9983-483f-99ff-aea130bca985"

metro_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\metro"
metro_stops_path = os.path.join(metro_dir, "metro_stops.shp")
output_dir = os.path.join(metro_dir, "isochrones")
output_file_path = os.path.join(output_dir, "metro_isochrones.shp")
last_processed_id_path = os.path.join(output_dir, "last_processed_metro_id.txt")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

metro_stops_gdf = gpd.read_file(metro_stops_path)

try:
    with open(last_processed_id_path, "r") as file:
        last_processed_id = file.read().strip()
except FileNotFoundError:
    last_processed_id = None

request_count = 0
minutely_limit = 10

for index, stop in metro_stops_gdf.iterrows():
    if last_processed_id is not None and stop['stop_id'] <= last_processed_id:
        continue

    if request_count >= minutely_limit:
        time.sleep(60)  # Wait for a minute if limit reached
        request_count = 0  # Reset request count after pausing

    isochrone_data, rate_limit_remaining = get_isochrone_data(api_key, stop['stop_lat'], stop['stop_lon'])
    request_count += 1

    if rate_limit_remaining < 200:
        print("Credits below 200, stopping.")
        break

    if isochrone_data:
        geom = shape(isochrone_data['polygons'][0]['geometry'])
        transformed_geom = transform_geometry(geom, "EPSG:4326", "EPSG:2180")
        stop['geometry'] = transformed_geom

        new_data = gpd.GeoDataFrame([stop], geometry=[transformed_geom], crs="EPSG:2180")

        if not os.path.exists(output_file_path):
            new_data.to_file(output_file_path, driver='ESRI Shapefile')
        else:
            new_data.to_file(output_file_path, mode='a', driver='ESRI Shapefile')

        with open(last_processed_id_path, "w") as file:
            file.write(stop['stop_id'])

        print(f"Processed stop ID: {stop['stop_id']}, Credits Remaining: {rate_limit_remaining}")
        time.sleep(2)  # 2-second pause between requests

    else:
        print(f"Error with stop at {stop['stop_lat']}, {stop['stop_lon']}")
        time.sleep(10)  # Wait before retrying in case of an error

print("Finished processing all metro stops.")