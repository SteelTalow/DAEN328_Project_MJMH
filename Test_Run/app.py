import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

 # Load env vars
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_NAME = os.getenv("POSTGRES_DB")
#establish connection paramaters with postgres
db_url = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

#db_url = r"postgresql://postgres:admin@db:5432/demo"


# Function to Connect to PostgreSQL
@st.cache_resource
def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        engine = create_engine(db_url)
        conn = engine.connect()
        return conn
    except Exception as e:
        st.error(f"❌ Error connecting to database: {e}")
        return None

conn = get_db_connection()

if conn:
    st.success("✅ Successfully connected to PostgreSQL database!")
else:
    st.error("❌ Failed to connect. Check your credentials.")

def fetch_data():
    """Fetch sample data from a PostgreSQL table and handle errors."""
    query = "SELECT * FROM combined_taxi_data LIMIT 10;"  # Replace 'your_table' with actual table name
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

data = fetch_data()
if not data.empty:
    st.write("### 📊 Sample Data from PostgreSQL")
    st.dataframe(data)
else:
    st.warning("⚠️ No data retrieved. Check your query or database connection.")

# Ensure Connection is Closed After Execution
conn.close()




