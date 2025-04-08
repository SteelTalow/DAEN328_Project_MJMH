import json
from datetime import datetime
import pandas as pd

def clean_record(record):
    # Convert types
    keys_to_int = ['vendorid', 'passenger_count', 'ratecodeid', 'pulocationid', 'dolocationid', 'payment_type']
    keys_to_float = ['trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 
                     'tolls_amount', 'improvement_surcharge', 'total_amount', 
                     'congestion_surcharge', 'airport_fee']
    
    for key in keys_to_int:
        if key in record:
            record[key] = int(float(record[key]))
    
    for key in keys_to_float:
        if key in record:
            record[key] = float(record[key])
    
    # Format datetime strings to remove milliseconds
    try:
        pickup_raw = record.get('tpep_pickup_datetime')
        dropoff_raw = record.get('tpep_dropoff_datetime')

        # Debugging: log the raw datetime strings
        print(f"Raw pickup: {pickup_raw}")
        print(f"Raw dropoff: {dropoff_raw}")

        # Use pandas to handle datetime parsing
        pickup = pd.to_datetime(pickup_raw, errors='coerce')  # `coerce` will return NaT on error
        dropoff = pd.to_datetime(dropoff_raw, errors='coerce')

        # Check if parsing failed
        if pd.isna(pickup) or pd.isna(dropoff):
            print(f"Parsing error: Invalid datetime format for pickup: {pickup_raw} or dropoff: {dropoff_raw}")
            record['trip_duration_minutes'] = None
        else:
            # Calculate the duration in minutes
            duration = (dropoff - pickup).total_seconds() / 60
            record['trip_duration_minutes'] = float(f'{duration:.2f}')

            # Optionally, overwrite with consistent formatting (if you want)
            record['tpep_pickup_datetime'] = pickup.strftime('%Y-%m-%dT%H:%M:%S')
            record['tpep_dropoff_datetime'] = dropoff.strftime('%Y-%m-%dT%H:%M:%S')

    except Exception as e:
        print("Datetime error:", e)
        record['trip_duration_minutes'] = None

    return record

# === Load, clean, and save ===

# Replace with your actual filename
input_filename = 'taxi_data.json'
output_filename = 'cleaned_taxi_data.json'

# Load the data from the JSON file
with open(input_filename, 'r') as f:
    data = json.load(f)

# If it's a list of records, clean each record
if isinstance(data, list):
    cleaned = [clean_record(record) for record in data]
else:
    cleaned = clean_record(data)

# Save the cleaned data back to JSON
with open(output_filename, 'w') as f:
    json.dump(cleaned, f, indent=2)

print("Cleaning complete. Saved to:", output_filename)



def find_nulls(record, path=""):
    nulls = []
    for key, value in record.items():
        full_path = f"{path}.{key}" if path else key
        if value is None:
            nulls.append(full_path)
        elif isinstance(value, dict):
            nulls.extend(find_nulls(value, full_path))
    return nulls

# === Search for nulls ===
all_nulls = []

if isinstance(data, list):
    for idx, record in enumerate(data):
        null_fields = find_nulls(record)
        if null_fields:
            all_nulls.append((idx, null_fields))
else:
    null_fields = find_nulls(data)
    if null_fields:
        all_nulls.append((0, null_fields))

# === Display Results ===
if all_nulls:
    for i, fields in all_nulls:
        print(f"Record {i} has nulls in fields: {fields}")
else:
    print("✅ No null values found.")
