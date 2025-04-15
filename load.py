import pandas as pd
from sqlalchemy import create_engine, text
import os
import pandas as pd
import psycopg2

def load_postgress(taxi_data):
    # Load env vars
    DB_HOST = os.getenv("POSTGRES_HOST")
    DB_PORT = os.getenv("POSTGRES_PORT")
    DB_USER = os.getenv("POSTGRES_USER")
    DB_PASS = os.getenv("POSTGRES_PASSWORD")
    DB_NAME = os.getenv("POSTGRES_DB")
    #establish connection paramaters with postgres
    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    df = pd.read_csv("/Json_data/cleaned_taxidata.csv") 
    #This creates the schema for the data to be loaded into
    with engine.connect() as conn:
        conn.execute(text("DROP TABLE IF EXISTS taxi_trips"))
        conn.execute(text("""
            CREATE TABLE combined_taxi_data(
            id SERIAL PRIMARY KEY,
            vendorid INTEGER,
            passenger_count INTEGER,
            trip_distance FLOAT,
            pulocationid INTEGER,
            dolocationid INTEGER,
            payment_type INTEGER,
            fare_amount FLOAT,
            extra FLOAT,
            mta_tax FLOAT,
            tip_amount FLOAT,
            tolls_amount FLOAT,
            improvement_surcharge FLOAT,
            total_amount FLOAT,
            pickup_date TIMESTAMP,
            pickup_time TIME,
            dropoff_date TIMESTAMP,
            dropoff_time TIME,
            total_time TIME               
        )
    """))
    #loads df into PostgresSQL
    df.to_sql('combined_taxi_data', engine, if_exists='replace', index=False)


