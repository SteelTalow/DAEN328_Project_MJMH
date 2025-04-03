import json  # To handle JSON data from APIs
import pandas as pd

json_path = r'taxi_data.json'

df = pd.read_json(json_path)

# Display the first few rows of the DataFrame
print(df.head())

print(df.columns)