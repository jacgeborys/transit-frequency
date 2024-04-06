import pandas as pd
from datetime import datetime, timedelta

# Load the frequencies data
df = pd.read_csv(r"C:\Users\Asus\OneDrive\Pulpit\Rozne\QGIS\TransitFrequency\_data\2024_03_27\frequencies.txt")

# Adjust time to handle "24:xx:xx", "25:xx:xx" formats and beyond
def adjust_time(time_str):
    # Handle times that go beyond 23:59:59
    parts = time_str.split(':')
    hours, minutes, seconds = int(parts[0]) % 24, int(parts[1]), int(parts[2])
    new_time_str = f"{hours:02}:{minutes:02}:{seconds:02}"
    # Adding days if original hour was >= 24 to correctly calculate timedelta later
    days_add = int(parts[0]) // 24
    time_obj = datetime.strptime(new_time_str, '%H:%M:%S') + timedelta(days=days_add)
    return time_obj

# Calculate trips within specified hours (6 AM to 10 PM)
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

# Calculate trips for each entry
df['trips'] = df.apply(calculate_trips, axis=1)

# Group by the full trip_id before any time or additional identifier
trips_per_day_full = df.groupby('trip_id')['trips'].sum().reset_index(name='total_trips')

print(trips_per_day_full)