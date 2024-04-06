import pandas as pd
import geopandas as gpd
import numpy as np
from sklearn.cluster import DBSCAN
from shapely.geometry import Point

def adjust_time(time_str):
    """Adjust times that are beyond 24:00:00 to be within a valid 24-hour range."""
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours >= 24:
        hours -= 24
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Define base path for easier updates
base_path = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\KM'

# File paths using base path
stops_file_path = f'{base_path}\\stops.txt'
stop_times_file_path = f'{base_path}\\stop_times.txt'
trips_file_path = f'{base_path}\\trips.txt'
calendar_dates_file_path = f'{base_path}\\calendar_dates.txt'

# Load data
stops_df = pd.read_csv(stops_file_path, dtype={'stop_id': str}, encoding='utf-8')
stop_times_df = pd.read_csv(stop_times_file_path, dtype={'stop_id': str}, encoding='utf-8')
trips_df = pd.read_csv(trips_file_path, encoding='utf-8')
calendar_dates_df = pd.read_csv(calendar_dates_file_path, encoding='utf-8')

# Filter for the specific date
calendar_dates_filtered = calendar_dates_df[calendar_dates_df['date'] == 20240327]
trips_filtered = trips_df.merge(calendar_dates_filtered, on='service_id')
stop_times_filtered = stop_times_df[stop_times_df['trip_id'].isin(trips_filtered['trip_id'])]

# Adjust and filter times
stop_times_filtered.loc[:, 'arrival_time'] = stop_times_filtered['arrival_time'].apply(adjust_time)
stop_times_filtered.loc[:, 'departure_time'] = stop_times_filtered['departure_time'].apply(adjust_time)
mask = stop_times_filtered['arrival_time'] < "22:00:00"
filtered_stop_times_df = stop_times_filtered[mask]

# Count trips and merge with stops
trip_counts = filtered_stop_times_df.groupby('stop_id').size().reset_index(name='trip_count')
stops_df = stops_df.merge(trip_counts, on='stop_id', how='left')
stops_df['trip_count'].fillna(0, inplace=True)

# Convert to GeoDataFrame in EPSG:4326
stops_gdf = gpd.GeoDataFrame(stops_df, geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat), crs="EPSG:4326")

# Project to a metric CRS for clustering
stops_gdf_metric = stops_gdf.to_crs(epsg=2180)

# Cluster stops using DBSCAN
coords = np.array(stops_gdf_metric.geometry.apply(lambda geom: (geom.x, geom.y)).tolist())
db = DBSCAN(eps=150, min_samples=1, metric='euclidean').fit(coords)
stops_gdf['cluster'] = db.labels_

# Aggregate trip_count by cluster
clustered = stops_gdf.dissolve(by='cluster', aggfunc={'trip_count': 'sum'})

# Calculate centroid for each cluster in metric CRS and then project back to WGS84 for compatibility
clustered['centroid'] = clustered.geometry.centroid
clustered_centroids_wgs84 = clustered.set_geometry('centroid').to_crs(epsg=4326)

# Project the centroids to EPSG:2180 for final output
clustered_centroids_final = clustered_centroids_wgs84.to_crs(epsg=2180)

# Prepare DataFrame for CSV output
clustered_df = pd.DataFrame(clustered_centroids_final.drop(columns=['geometry', 'centroid']))
clustered_df['stop_lat'] = clustered_centroids_wgs84.geometry.y
clustered_df['stop_lon'] = clustered_centroids_wgs84.geometry.x
clustered_df.reset_index(inplace=True)
clustered_df['train'] = clustered_df['trip_count']

# Save to CSV for later use in QGIS or other GIS software
output_file_path = f'{base_path}\\stops_trip_count_KM.csv'
clustered_df.to_csv(output_file_path, index=False, encoding='utf-8')

print(f"Saved to '{output_file_path}'")