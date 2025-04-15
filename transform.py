import json
import pandas as pd
import numpy as np
import csv

taxi_2019_data = "Json_data/api_data_taxi_2019.json"
taxi_2021_data = "Json_data/api_data_taxi_2021.json"
taxi_2023_data = "Json_data/api_data_taxi_2023.json"

df_2019 = pd.read_json(taxi_2019_data)
df_2021 = pd.read_json(taxi_2021_data)
df_2023 = pd.read_json(taxi_2023_data)

df = pd.concat([df_2019, df_2021, df_2023], ignore_index=True)
df.to_json("combined.json", orient='records', lines=False)

df.to_csv("taxidata.csv", index=False)

#might have to drop ratecodeid and store_and_fwd_flag
df = df.drop(columns = ["congestion_surcharge","airport_fee"])

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

def defaultPassenger(passengerCount):
    if pd.isnull(passengerCount):
        count = 1
    else:
        count = passengerCount
    return count

df["passenger_count"] = df["passenger_count"].apply(defaultPassenger)

def defaultPaymentType(payment):
    if pd.isnull(payment):
        pay = 5
    else:
        pay = payment
    return pay
df["payment_type"] = df["payment_type"].apply(defaultPaymentType)

def costPerMile(totalAmount, tripDistance):
    cpm = totalAmount/tripDistance
    return cpm
df["cost_per_mile"] = df.apply(
    lambda row: row["total_amount"] / row["trip_distance"] if row["trip_distance"] > 0 else 0,
    axis=1
)


# def costPerMinute

df = df[~(df['vendorid'].isnull() | (df['vendorid'] == ''))]       
    
df = df.drop(columns = ["tpep_pickup_datetime","tpep_dropoff_datetime", "ratecodeid","store_and_fwd_flag"])
print(df.head())
df.to_csv(r"cleaned_taxidata.csv", index=False)