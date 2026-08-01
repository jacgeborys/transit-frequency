import requests
import geopandas as gpd
import pandas as pd
import time
from shapely.geometry import shape
import os

def get_time_limit(railway):
    if railway == 'tram':
        return 480  # 8 minutes in seconds
    elif railway in ['subway', 'light_rail', 'monorail']:
        return 720  # 12 minutes in seconds
    elif railway == 'train':
        return 900  # 15 minutes in seconds
    else:
        return 480  # default time

def get_isochrone_data(api_key, latitude, longitude, railway_type):
    time_limit = get_time_limit(railway_type)
    url = f"https://graphhopper.com/api/1/isochrone?point={latitude},{longitude}&time_limit={time_limit}&vehicle=foot&key={api_key}"
    response = requests.get(url)
    return response

# Load tram stops from file
file_path = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\Git\Rail_transit_availability\SHP\Warszawa\stops_M3.shp"
tram_stops = gpd.read_file(file_path)

# GraphHopper API key
api_key = "4043f424-40d7-4351-bf7e-bb3ec47e8e28"

isochrone_data = []
last_processed_id = 0
last_processed_index_path = "last_processed_index.txt"

try:
    with open(last_processed_index_path, "r") as file:
        last_processed_id = int(file.read())
except FileNotFoundError:
    pass

output_file_path = r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\Git\Rail_transit_availability\SHP\Warszawa\isochrones_M3.shp"

if os.path.exists(output_file_path):
    existing_data = gpd.read_file(output_file_path)
else:
    existing_data = gpd.GeoDataFrame(columns=['geometry', 'railway'])

for _, stop in tram_stops[tram_stops['id'] > last_processed_id].iterrows():
    response = get_isochrone_data(api_key, stop['latitude'], stop['longitude'], stop['railway'])
    rate_limit_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))

    if response.status_code == 200:
        isochrone_json = response.json()
        for polygon in isochrone_json['polygons']:
            geom = shape(polygon['geometry'])
            isochrone_data.append({'geometry': geom, 'railway': stop['railway'], 'id': stop['id']})

        print(f"Processed stop with ID: {stop['id']}")
        new_data = gpd.GeoDataFrame(isochrone_data, geometry='geometry')
        combined_data = pd.concat([existing_data, new_data])
        combined_data.to_file(output_file_path)

        with open(last_processed_index_path, "w") as file:
            file.write(str(stop['id']))

        print(f"Rate Limit Remaining: {rate_limit_remaining}")
        if rate_limit_remaining <= 150:
            print("Approaching rate limit, stopping requests.")
            break

    else:
        print(f"Error with stop at {stop['latitude']}, {stop['longitude']}: {response.text}")
        time.sleep(60)  # Wait before retrying

    time.sleep(15)  # Sleep between different stops