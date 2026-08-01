"""
Calculate trip counts per stop for KM (Regional Rail).
Clusters nearby stops with DBSCAN and deduplicates by route_id.
"""
import pandas as pd
import geopandas as gpd
import numpy as np
from sklearn.cluster import DBSCAN
from shapely.geometry import Point

BASE_PATH = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\KM'


def adjust_time(time_str):
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours >= 24:
        hours -= 24
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


# Load data
stops_df = pd.read_csv(f'{BASE_PATH}\\stops.txt', dtype={'stop_id': str}, encoding='utf-8')
stop_times_df = pd.read_csv(f'{BASE_PATH}\\stop_times.txt', dtype={'stop_id': str}, encoding='utf-8')
trips_df = pd.read_csv(f'{BASE_PATH}\\trips.txt', dtype={'route_id': str}, encoding='utf-8')
calendar_dates_df = pd.read_csv(f'{BASE_PATH}\\calendar_dates.txt', encoding='utf-8')

# Filter for specific date
calendar_filtered = calendar_dates_df[calendar_dates_df['date'] == 20240327]
trips_filtered = trips_df.merge(calendar_filtered, on='service_id')
stop_times_filtered = stop_times_df[stop_times_df['trip_id'].isin(trips_filtered['trip_id'])]

# Map trip_id -> route_id
trip_route = trips_filtered[['trip_id', 'route_id']].drop_duplicates()
stop_times_filtered = stop_times_filtered.merge(trip_route, on='trip_id', how='left')

# Adjust and filter times
stop_times_filtered = stop_times_filtered.copy()
stop_times_filtered['arrival_time'] = stop_times_filtered['arrival_time'].apply(adjust_time)
stop_times_filtered['departure_time'] = stop_times_filtered['departure_time'].apply(adjust_time)
mask = (stop_times_filtered['arrival_time'] >= "06:00:00") & (stop_times_filtered['arrival_time'] < "22:00:00")
filtered = stop_times_filtered[mask]

# Count trips and unique routes per stop
trip_counts = filtered.groupby('stop_id').size().reset_index(name='trip_count')
unique_routes = filtered.groupby('stop_id')['route_id'].nunique().reset_index(name='unique_routes')
route_lists = filtered.groupby('stop_id')['route_id'].apply(
    lambda x: ','.join(sorted(x.unique()))
).reset_index(name='route_ids')

stops_df = stops_df.merge(trip_counts, on='stop_id', how='left')
stops_df = stops_df.merge(unique_routes, on='stop_id', how='left')
stops_df = stops_df.merge(route_lists, on='stop_id', how='left')
stops_df['trip_count'] = stops_df['trip_count'].fillna(0)
stops_df['unique_routes'] = stops_df['unique_routes'].fillna(0).astype(int)

# Convert to GeoDataFrame
stops_gdf = gpd.GeoDataFrame(
    stops_df,
    geometry=gpd.points_from_xy(stops_df.stop_lon, stops_df.stop_lat),
    crs="EPSG:4326"
)

# Cluster stops using DBSCAN (150m threshold)
stops_metric = stops_gdf.to_crs(epsg=2180)
coords = np.array(stops_metric.geometry.apply(lambda g: (g.x, g.y)).tolist())
db = DBSCAN(eps=150, min_samples=1, metric='euclidean').fit(coords)
stops_gdf['cluster'] = db.labels_

# Aggregate per cluster: sum trip_count, union route_ids (deduplicated)
def agg_routes(series):
    all_routes = set()
    for val in series.dropna():
        all_routes.update(val.split(','))
    return ','.join(sorted(all_routes))

cluster_agg = stops_gdf.groupby('cluster').agg(
    trip_count=('trip_count', 'sum'),
    route_ids=('route_ids', agg_routes),
).reset_index()
cluster_agg['unique_routes'] = cluster_agg['route_ids'].apply(
    lambda x: len(x.split(',')) if x else 0
)

# Calculate centroid per cluster
clustered = stops_gdf.dissolve(by='cluster')
clustered['centroid'] = clustered.geometry.centroid
centroids_wgs84 = clustered.set_geometry('centroid').to_crs(epsg=4326)

# Build output
clustered_df = pd.DataFrame({
    'cluster': centroids_wgs84.index,
    'stop_lat': centroids_wgs84.geometry.y.values,
    'stop_lon': centroids_wgs84.geometry.x.values,
})
clustered_df = clustered_df.merge(cluster_agg, on='cluster')
clustered_df['train'] = clustered_df['trip_count']

output_file_path = f'{BASE_PATH}\\stops_trip_count_KM.csv'
clustered_df.to_csv(output_file_path, index=False, encoding='utf-8')
print(f"Saved {len(clustered_df)} clusters to '{output_file_path}'")
print(f"  Total departures: {int(clustered_df['trip_count'].sum())}")
print(f"  Unique routes: {filtered['route_id'].nunique()}")
