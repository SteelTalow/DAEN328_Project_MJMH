import pandas as pd
from sqlalchemy import create_engine
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

    db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(db_url)

    df = pd.read_csv("/app/data/cleaned_taxidata.csv")

    




#Code below will upload without first needing to make schema. Investigate whether that is what we want

#make sure to figure out exactly how/what the connection paramaters will be

# Load your CSV
df = pd.read_csv('your_file.csv')

# Create DB connection (edit user/pass/db/host as needed)
engine = create_engine('postgresql://username:password@localhost:5432/your_db')

# Send to Postgres (will auto-create table)
df.to_sql('your_table_name', engine, if_exists='replace', index=False)
