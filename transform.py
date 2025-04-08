import json
import pandas as pd
import numpy 
import csv

filename = "taxi_data.json"
df = pd.read_json(filename)
df.to_csv('taxidata.csv', index=False)

def dropAirport(df):
    if "airport_fee" in df.columns:
        df = df.drop(columns = ["airport_fee"])

dropAirport(df)

def seperatePickup(date):
    split_array = date.split("T")
    # print(split_array)
    Pickup_Date = split_array[0]
    Pickup_Time = split_array[1].split(".")[0]
    return pd.Series([Pickup_Date, Pickup_Time])

df[["pickup_date", "pickup_time"]] = df["tpep_pickup_datetime"].apply(seperatePickup)

def seperateDropoff(date):
    split_array = date.split("T")
    # print(split_array)
    dropoff_date = split_array[0]
    dropoff_time = split_array[1].split(".")[0]
    return pd.Series([dropoff_date, dropoff_time])

df[["dropoff_date", "dropoff_time"]] = df["tpep_dropoff_datetime"].apply(seperatePickup)

def totalTime(pickuptime, dropofftime):
    pickup_time = pd.to_datetime(pickuptime, format="%H:%M:%S")
    dropoff_time = pd.to_datetime(dropofftime, format="%H:%M:%S")
    total_time = dropoff_time - pickup_time
    return total_time
df["total_time"] = df.apply(lambda row: totalTime(row["pickup_time"], row["dropoff_time"]), axis=1)


print(df.head())