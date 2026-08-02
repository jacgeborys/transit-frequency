"""
Calculate trip counts per stop for WKD (Suburban Rail).
Deduplicates by route_id to avoid counting the same line from nearby stops.
"""
import pandas as pd

BASE_PATH = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\WKD'


def adjust_time(time_str):
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours >= 24:
        hours -= 24
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


# Load the data
stops_df = pd.read_csv(f'{BASE_PATH}\\stops.txt')
stop_times_df = pd.read_csv(f'{BASE_PATH}\\stop_times.txt', dtype={'stop_id': str})
trips_df = pd.read_csv(f'{BASE_PATH}\\trips.txt', dtype={'route_id': str})

# Map trip_id -> route_id
trip_route = trips_df[['trip_id', 'route_id']].drop_duplicates()
stop_times_df = stop_times_df.merge(trip_route, on='trip_id', how='left')

# Adjust times and filter
stop_times_df['arrival_time'] = stop_times_df['arrival_time'].apply(adjust_time)
stop_times_df['departure_time'] = stop_times_df['departure_time'].apply(adjust_time)
mask = (stop_times_df['arrival_time'] >= "06:00:00") & (stop_times_df['arrival_time'] < "22:00:00")
filtered = stop_times_df[mask]

# Count trips per stop
trip_counts = filtered['stop_id'].value_counts().reset_index()
trip_counts.columns = ['stop_id', 'trip_count']

# Count unique routes per stop
unique_routes = filtered.groupby('stop_id')['route_id'].nunique().reset_index(name='unique_routes')
route_lists = filtered.groupby('stop_id')['route_id'].apply(
    lambda x: ','.join(sorted(x.unique()))
).reset_index(name='route_ids')

# Merge
stops_df = stops_df.merge(trip_counts, on='stop_id', how='left')
stops_df = stops_df.merge(unique_routes, on='stop_id', how='left')
stops_df = stops_df.merge(route_lists, on='stop_id', how='left')
stops_df['trip_count'] = stops_df['trip_count'].fillna(0).astype(int)
stops_df['unique_routes'] = stops_df['unique_routes'].fillna(0).astype(int)
stops_df = stops_df[stops_df['trip_count'] > 0]
stops_df['train'] = stops_df['trip_count']

output_file_path = f'{BASE_PATH}\\stops_trip_count_wkd.csv'
stops_df.to_csv(output_file_path, index=False, encoding='utf-8')
print(f"Saved {len(stops_df)} stops to '{output_file_path}'")
print(f"  Total departures: {stops_df['trip_count'].sum()}")
print(f"  Unique routes: {filtered['route_id'].nunique()}")
