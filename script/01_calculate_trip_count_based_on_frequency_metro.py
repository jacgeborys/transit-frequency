"""
Calculate trip counts for Metro based on frequencies.txt.
Metro uses headway-based scheduling rather than explicit stop_times.
Extracts route_ids (M1, M2) from trip_id for deduplication.
"""
import pandas as pd
from datetime import datetime, timedelta

BASE_PATH = r'C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27'

# Load the frequencies data (metro uses ZTM's frequencies.txt)
df = pd.read_csv(f'{BASE_PATH}\\ZTM\\frequencies.txt')


def adjust_time(time_str):
    parts = time_str.split(':')
    hours, minutes, seconds = int(parts[0]) % 24, int(parts[1]), int(parts[2])
    new_time_str = f"{hours:02}:{minutes:02}:{seconds:02}"
    days_add = int(parts[0]) // 24
    time_obj = datetime.strptime(new_time_str, '%H:%M:%S') + timedelta(days=days_add)
    return time_obj


def calculate_trips(row):
    morning_bound = row['start_time'].replace(hour=6, minute=0, second=0, microsecond=0)
    evening_bound = row['start_time'].replace(hour=22, minute=0, second=0, microsecond=0)
    start_time = max(row['start_time'], morning_bound)
    end_time = min(row['end_time'], evening_bound)
    if start_time >= end_time:
        return 0
    duration_seconds = (end_time - start_time).total_seconds()
    return max(0, round(duration_seconds / row['headway_secs']))


# Apply time adjustments
df['start_time'] = df['start_time'].apply(adjust_time)
df['end_time'] = df['end_time'].apply(adjust_time)

# Extract route_id from trip_id (e.g., "M1/TP-MLO/M1" -> "M1")
df['route_id'] = df['trip_id'].str.split('/').str[0]

# Calculate trips for each entry
df['trips'] = df.apply(calculate_trips, axis=1)

# Group by trip_id
trips_per_day = df.groupby('trip_id').agg(
    total_trips=('trips', 'sum'),
    route_id=('route_id', 'first'),
).reset_index()

# Also produce a per-route summary
route_summary = trips_per_day.groupby('route_id')['total_trips'].sum().reset_index()
route_ids_str = ','.join(sorted(route_summary['route_id'].unique()))

print("Metro trip counts per route:")
print(route_summary.to_string(index=False))
print(f"\nRoute IDs: {route_ids_str}")
print(f"Total trips (6 AM - 10 PM): {int(route_summary['total_trips'].sum())}")

# Save for reference
trips_per_day.to_csv(f'{BASE_PATH}\\metro\\metro_trip_counts.csv', index=False)
print(f"\nSaved to {BASE_PATH}\\metro\\metro_trip_counts.csv")
