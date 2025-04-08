import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine


DB_PARAMS = {
    "dbname": "demo",
    "user": "postgres",
    "password": "admin",
    "host": "localhost",  # Change if hosted remotely
    "port": "5432"
}

# Function to Connect to PostgreSQL
@st.cache_resource
def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_PARAMS['user']}:{DB_PARAMS['password']}@"
            f"{DB_PARAMS['host']}:{DB_PARAMS['port']}/{DB_PARAMS['dbname']}"
        )
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
    query = "SELECT * FROM quotes LIMIT 10;"  # Replace 'your_table' with actual table name
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




