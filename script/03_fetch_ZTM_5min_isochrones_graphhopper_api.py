import requests
import geopandas as gpd
from shapely.geometry import shape
import time
import os
from shapely.ops import transform
from functools import partial
from pyproj import Transformer

def get_isochrone_data(api_key, latitude, longitude):
    time_limit = 300  # 5 minutes in seconds
    url = f"https://graphhopper.com/api/1/isochrone?point={latitude},{longitude}&time_limit={time_limit}&vehicle=foot&key={api_key}"
    response = requests.get(url)
    return response, int(response.headers.get('X-RateLimit-Remaining', 0))

def transform_geometry(geometry, src_crs, dst_crs):
    transformer = Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return transform(transformer.transform, geometry)

api_key = "e63ec932-9983-483f-99ff-aea130bca985" #1
# api_key = "e3059137-abdb-4758-a175-e74b8254b6d7" #2

base_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27"
stops_file_path = os.path.join(base_dir, "stops_trip_count_ZTM.shp")
output_file_path = os.path.join(base_dir, "isochrones", "ZTM_isochrones.shp")
last_processed_id_path = os.path.join(base_dir, "isochrones", "last_processed_id.txt")

if not os.path.exists(os.path.dirname(output_file_path)):
    os.makedirs(os.path.dirname(output_file_path))

stops_gdf = gpd.read_file(stops_file_path)

try:
    with open(last_processed_id_path, "r") as file:
        last_processed_id = file.read().strip()
except FileNotFoundError:
    last_processed_id = None

request_count = 0
minutely_limit = 10

for index, stop in stops_gdf.iterrows():
    if last_processed_id is not None and stop['stop_id'] <= last_processed_id:
        continue

    if request_count >= minutely_limit:
        time.sleep(60)  # Wait for a minute if limit reached
        request_count = 0  # Reset request count after pausing

    response, rate_limit_remaining = get_isochrone_data(api_key, stop['stop_lat'], stop['stop_lon'])
    request_count += 1

    if rate_limit_remaining < 200:  # Check if rate limit remaining crosses below 200
        print("Credits below 200, stopping.")
        break  # Stop the loop

    if response.status_code == 200:
        isochrone_json = response.json()
        geom = shape(isochrone_json['polygons'][0]['geometry'])
        transformed_geom = transform_geometry(geom, "EPSG:4326", "EPSG:2180")
        stop['geometry'] = transformed_geom

        # Create a temporary GeoDataFrame to hold current isochrone
        temp_gdf = gpd.GeoDataFrame([stop], geometry=[transformed_geom], crs="EPSG:2180")

        # If output file doesn't exist, initialize it with current isochrone
        if not os.path.exists(output_file_path):
            temp_gdf.to_file(output_file_path, driver='ESRI Shapefile')
        else:
            # Append to existing file
            temp_gdf.to_file(output_file_path, mode='a', driver='ESRI Shapefile')

        with open(last_processed_id_path, "w") as file:
            file.write(stop['stop_id'])

        print(f"Processed stop ID: {stop['stop_id']}, Credits Remaining: {rate_limit_remaining}")
        time.sleep(2)

    else:
        print(f"Error with stop at {stop['stop_lat']}, {stop['stop_lon']}: {response.text}")
        # Wait before retrying in case of error
        time.sleep(10)

print("Finished processing all stops.")