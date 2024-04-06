import pandas as pd

def adjust_time(time_str):
    """Adjust times that are beyond 24:00:00 to be within a valid 24-hour range."""
    hours, minutes, seconds = map(int, time_str.split(":"))
    if hours >= 24:
        hours -= 24
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

# Adjusted file paths for the new dataset
stops_file_path = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\WKD\stops.txt'
stop_times_file_path = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\WKD\stop_times.txt'

# Load the data
stops_df = pd.read_csv(stops_file_path)
stop_times_df = pd.read_csv(stop_times_file_path, dtype={'stop_id': str})

# Adjust 'arrival_time' and 'departure_time' for values beyond "24:00:00"
stop_times_df['arrival_time'] = stop_times_df['arrival_time'].apply(adjust_time)
stop_times_df['departure_time'] = stop_times_df['departure_time'].apply(adjust_time)

# Filter to include only times up to "22:00:00"
mask = stop_times_df['arrival_time'] < "22:00:00"
filtered_stop_times_df = stop_times_df[mask]

# Count the number of trips per stop_id in the filtered stop_times
trip_counts = filtered_stop_times_df['stop_id'].value_counts().reset_index()
trip_counts.columns = ['stop_id', 'trip_count']

# Merge the counts back into the stops dataframe
stops_df = pd.merge(stops_df, trip_counts, on='stop_id', how='left')

# Fill NaN values with 0 for stops without any trips and remove those rows
stops_df['trip_count'] = stops_df['trip_count'].fillna(0).astype(int)
stops_df = stops_df[stops_df['trip_count'] > 0]
stops_df['train'] = stops_df['trip_count']

# Specify the file path for the output
output_file_path = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\WKD\stops_trip_count_wkd.csv'

# Save the DataFrame to a CSV file
stops_df.to_csv(output_file_path, index=False, encoding='utf-8')

print(f"Saved to '{output_file_path}'")