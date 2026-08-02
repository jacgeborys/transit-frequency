"""
Calculate trip counts per stop for ZTM (Warsaw Public Transport).
Counts unique routes per stop to avoid double-counting the same line.
Also records total departures (trip_count) for frequency reference.
"""
import pandas as pd
import re

DATE_FILTER = "RA240327"
BASE_PATH = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\ZTM'


def adjust_time(time_str):
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours >= 24:
        hours -= 24
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def vehicle_type(trip_id):
    if re.search(r'/\d{1,2}/', trip_id):
        return 'tram'
    elif re.search(r'/\d{3}/', trip_id) or re.search(r'/(L|E|Z)[-\w]{1,3}/', trip_id):
        return 'bus'
    elif re.search(r'/S\d{1,2}/', trip_id):
        return 'train'
    elif re.search(r'/N\d{1,2}/', trip_id):
        return 'night_bus'
    return 'Unknown'


# Load the data
stops_df = pd.read_csv(f'{BASE_PATH}\\stops.txt', dtype={'stop_id': str})
stop_times_df = pd.read_csv(f'{BASE_PATH}\\stop_times.txt', dtype={'stop_id': str})
trips_df = pd.read_csv(f'{BASE_PATH}\\trips.txt', dtype={'route_id': str})

# Filter for specific date
stop_times_df = stop_times_df[stop_times_df['trip_id'].str.contains(DATE_FILTER)]

# Map trip_id -> route_id
trip_route = trips_df[['trip_id', 'route_id']].drop_duplicates()
stop_times_df = stop_times_df.merge(trip_route, on='trip_id', how='left')

# Apply vehicle_type function
stop_times_df['vehicle_type'] = stop_times_df['trip_id'].apply(vehicle_type)

# Adjust times and filter to business hours
stop_times_df['arrival_time'] = stop_times_df['arrival_time'].apply(adjust_time)
stop_times_df['departure_time'] = stop_times_df['departure_time'].apply(adjust_time)
mask = (stop_times_df['arrival_time'] >= "06:00:00") & (stop_times_df['arrival_time'] < "22:00:00")
filtered = stop_times_df[mask]

# Count total departures per stop per vehicle type (for frequency reference)
trip_counts = filtered.groupby(['stop_id', 'vehicle_type']).size().unstack(fill_value=0).reset_index()

# Count unique routes per stop (the main metric — avoids double-counting same line)
unique_routes = filtered.groupby('stop_id')['route_id'].nunique().reset_index(name='unique_routes')

# Collect route_ids as comma-separated string per stop
route_lists = filtered.groupby('stop_id')['route_id'].apply(
    lambda x: ','.join(sorted(x.unique()))
).reset_index(name='route_ids')

# Merge everything into stops
stops_df = stops_df.merge(trip_counts, how='left', on='stop_id')
stops_df = stops_df.merge(unique_routes, how='left', on='stop_id')
stops_df = stops_df.merge(route_lists, how='left', on='stop_id')

stops_df.fillna(0, inplace=True)

for col in ['bus', 'tram', 'night_bus', 'train']:
    if col not in stops_df.columns:
        stops_df[col] = 0
    else:
        stops_df[col] = stops_df[col].astype(int)

stops_df['unique_routes'] = stops_df['unique_routes'].astype(int)
stops_df['trip_count'] = stops_df['bus'] + stops_df['tram'] + stops_df['train']

# Remove stops with no daytime service
stops_df = stops_df[stops_df['trip_count'] > 0]

output_file_path = f'{BASE_PATH}\\stops_trip_count_ZTM.csv'
stops_df.to_csv(output_file_path, index=False)
print(f"Saved {len(stops_df)} stops to '{output_file_path}'")
print(f"  Total departures: {stops_df['trip_count'].sum()}")
print(f"  Unique routes across all stops: {filtered['route_id'].nunique()}")
