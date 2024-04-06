import pandas as pd
import re

DATE_FILTER = "RA240327"  # Date to filter by

def adjust_time(time_str):
    """Adjust times that are beyond 24:00:00 to be within a valid 24-hour range."""
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours >= 24:
        hours -= 24
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def vehicle_type(trip_id):
    """Determine the vehicle type based on trip_id."""
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
stops_df = pd.read_csv(r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\stops.txt', dtype={'stop_id': str})
stop_times_df = pd.read_csv(r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\stop_times.txt', dtype={'stop_id': str})

# Filter stop_times_df to only include trips on the specified date
stop_times_df = stop_times_df[stop_times_df['trip_id'].str.contains(DATE_FILTER)]

# Apply vehicle_type function
stop_times_df['vehicle_type'] = stop_times_df['trip_id'].apply(vehicle_type)

# Adjust 'arrival_time' and 'departure_time'
stop_times_df['arrival_time'] = stop_times_df['arrival_time'].apply(adjust_time)
stop_times_df['departure_time'] = stop_times_df['departure_time'].apply(adjust_time)

# Filter to include only times up to "22:00:00"
mask = stop_times_df['arrival_time'] < "22:00:00"
filtered_stop_times_df = stop_times_df[mask]

# Count the number of trips per stop_id for each vehicle type
trip_counts = filtered_stop_times_df.groupby(['stop_id', 'vehicle_type']).size().unstack(fill_value=0).reset_index()

# Merge the counts back into the stops dataframe
stops_df = pd.merge(stops_df, trip_counts, how='left', on='stop_id')

# Fill NaN values with 0 for stops without any trips
stops_df.fillna(0, inplace=True)

# Ensure columns for bus, tram, night_bus exist and are integers
for col in ['bus', 'tram', 'night_bus', 'train']:
    if col not in stops_df.columns:
        stops_df[col] = 0
    else:
        stops_df[col] = stops_df[col].astype(int)

# Calculate total trip count as the sum of 'Bus', 'Tram', and 'Train' counts, excluding 'Night Bus'
stops_df['trip_count'] = stops_df['bus'] + stops_df['tram'] + stops_df['train']

# Remove rows where trip_count equals 0 (excluding night bus)
stops_df = stops_df[stops_df['trip_count'] > 0]

# Specify your file path
output_file_path = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\stops_trip_count_ZTM.csv'

# Save the DataFrame to a CSV file
stops_df.to_csv(output_file_path, index=False)

print(f"Saved to '{output_file_path}'")
