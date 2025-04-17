import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import plotly.express as px

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

# Convert the dropoff_date column to datetime
data['dropoff_date'] = pd.to_datetime(data['dropoff_date'], errors='coerce')

# Drop rows with invalid or missing dates
data = data.dropna(subset=['dropoff_date'])

# Extract year and month
data['year'] = data['dropoff_date'].dt.year
data['month'] = data['dropoff_date'].dt.month
data['month_name'] = data['dropoff_date'].dt.strftime('%b')

# Group by year and month
monthly_rides = data.groupby(['year', 'month_name']).size().reset_index(name='ride_count')

# Ensure month order is correct
month_order = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
monthly_rides['month_name'] = pd.Categorical(monthly_rides['month_name'], categories=month_order, ordered=True)
monthly_rides = monthly_rides.sort_values(['month_name'])

# Add checkboxes for years
st.sidebar.markdown("### 📅 Choose Years to Display")
years_available = [2019, 2021, 2023]
selected_years = [year for year in years_available if st.sidebar.checkbox(str(year), value=True)]

# Filter data
filtered_data = monthly_rides[monthly_rides['year'].isin(selected_years)]

if not filtered_data.empty:
    # Plot using Plotly
    fig = px.bar(filtered_data, x='month_name', y='ride_count', color='year',
                 barmode='group',
                 labels={'ride_count': 'Number of Rides', 'month_name': 'Month'},
                 title='📊 Monthly Ride Count Comparison')
    st.plotly_chart(fig)
else:
    st.warning("⚠️ No data selected. Please choose at least one year from the sidebar.")






# Ensure Connection is Closed After Execution

#conn.close()



