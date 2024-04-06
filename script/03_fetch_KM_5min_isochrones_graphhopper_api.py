import requests
import geopandas as gpd
from shapely.geometry import shape
import time
import os
from shapely.ops import transform
import pyproj

def get_isochrone_data(api_key, latitude, longitude):
    time_limit = 300  # 5 minutes in seconds
    url = f"https://graphhopper.com/api/1/isochrone?point={latitude},{longitude}&time_limit={time_limit}&vehicle=foot&key={api_key}"
    response = requests.get(url)
    # Extract the remaining credits from the response headers
    credits_remaining = response.headers.get('X-RateLimit-Remaining', 0)
    return response.json() if response.status_code == 200 else None, int(credits_remaining)

def transform_geometry(geometry, src_crs, dst_crs):
    transformer = pyproj.Transformer.from_crs(src_crs, dst_crs, always_xy=True)
    return transform(transformer.transform, geometry)

api_key = "e63ec932-9983-483f-99ff-aea130bca985"
base_dir = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\KM"
stops_file_path = os.path.join(base_dir, "stops_trip_count_KM.shp")
output_dir = os.path.join(base_dir, "isochrones")
output_file_path = os.path.join(output_dir, "KM_isochrones.shp")
last_processed_id_path = os.path.join(output_dir, "last_processed_id.txt")

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

stops_gdf = gpd.read_file(stops_file_path)

try:
    with open(last_processed_id_path, "r") as file:
        last_index_processed = int(file.read().strip())
except FileNotFoundError:
    last_index_processed = -1

request_count = 0
minutely_limit = 10
total_credits_used = 0

for index, stop in stops_gdf.iterrows():
    if index <= last_index_processed:
        continue

    # Throttle requests to stay within rate limits
    if request_count == minutely_limit:
        time.sleep(60)
        request_count = 0
    request_count += 1

    isochrone_data, credits_remaining = get_isochrone_data(api_key, stop['stop_lat'], stop['stop_lon'])
    total_credits_used += (total_credits_used - credits_remaining)

    if isochrone_data:
        geom = shape(isochrone_data['polygons'][0]['geometry'])
        transformed_geom = transform_geometry(geom, "EPSG:4326", "EPSG:2180")
        # Update stop geometry with the isochrone
        stop['geometry'] = transformed_geom
        new_data = gpd.GeoDataFrame([stop], geometry=[transformed_geom], crs="EPSG:2180")
        if index == 0 or not os.path.exists(output_file_path):
            new_data.to_file(output_file_path, driver='ESRI Shapefile')
        else:
            new_data.to_file(output_file_path, mode='a', driver='ESRI Shapefile')
        with open(last_processed_id_path, "w") as file:
            file.write(str(index))
        print(f"Processed stop ID: {stop['stop_id']}, Credits Remaining: {credits_remaining}")
        if credits_remaining < 200 or total_credits_used > 150000:
            print("Approaching credit limit, stopping early.")
            break
    else:
        print("Error fetching isochrone data.")
    time.sleep(2)

print("Finished processing all stops.")