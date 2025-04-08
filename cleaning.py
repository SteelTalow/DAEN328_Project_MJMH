import json
from datetime import datetime

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
    datetime_keys = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
    for key in datetime_keys:
        if key in record:
            try:
                dt = datetime.fromisoformat(record[key])
                record[key] = dt.strftime('%Y-%m-%dT%H:%M:%S')
            except ValueError:
                pass  # Keep original if it fails

    return record

# === Load, clean, and save ===

# Replace with your actual filename
input_filename = 'taxi_data.json'
output_filename = 'cleaned_taxi_data.json'

with open(input_filename, 'r') as f:
    data = json.load(f)

# If it's a list of records
if isinstance(data, list):
    cleaned = [clean_record(record) for record in data]
else:
    cleaned = clean_record(data)

with open(output_filename, 'w') as f:
    json.dump(cleaned, f, indent=2)

print("Cleaning complete. Saved to:", output_filename)


import json

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
