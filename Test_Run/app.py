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

def fetch_all_data():
    """Fetch all data from a PostgreSQL table and handle errors."""
    query = "SELECT * FROM combined_taxi_data;"  # Replace 'your_table' with actual table name
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"❌ Error fetching data: {e}")
        return pd.DataFrame()

data_s = fetch_data()
if not data_s.empty:
    st.write("### 📊 Sample Data from PostgreSQL")
    st.dataframe(data_s)
else:
    st.warning("⚠️ No data retrieved. Check your query or database connection.")
data = fetch_all_data()
data['dropoff_date'] = pd.to_datetime(data['dropoff_date'], errors='coerce')
data = data.dropna(subset=['dropoff_date'])

# Step 2: Extract year and month
data['year'] = data['dropoff_date'].dt.year
data['month'] = data['dropoff_date'].dt.month
data['month_name'] = data['dropoff_date'].dt.strftime('%b')

# Step 3: Filter years via checkboxes
st.sidebar.markdown("### 📅 Choose Years to Display")
years_available = [2019, 2021, 2023]
selected_years = [year for year in years_available if st.sidebar.checkbox(str(year), value=True)]

# Step 4: Filter dataset for selected years
filtered_data = data[data['year'].isin(selected_years)]

# Step 5: Build a complete DataFrame with 12 months per year (fill missing months with 0)
all_combinations = pd.MultiIndex.from_product(
    [selected_years, range(1, 13)], names=['year', 'month']
).to_frame(index=False)

# Merge with actual ride counts
monthly_counts = (
    filtered_data.groupby(['year', 'month'])
    .size()
    .reset_index(name='ride_count')
)

full_data = pd.merge(all_combinations, monthly_counts, on=['year', 'month'], how='left')
full_data['ride_count'] = full_data['ride_count'].fillna(0).astype(int)
full_data['month_name'] = full_data['month'].apply(lambda m: ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][m-1])
full_data['month_name'] = pd.Categorical(full_data['month_name'],
                                         categories=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
                                         ordered=True)

# Step 6: Plot
if not selected_years:
    st.warning("⚠️ No year selected. Please check at least one year to show data.")
else:
    full_data['year'] = full_data['year'].astype(str)  # Make it categorical
    fig = px.bar(
        full_data,
        x='month_name',
        y='ride_count',
        color='year',
        barmode='group',
        title='📊 Monthly Ride Count Comparison',
        labels={'ride_count': 'Number of Rides', 'month_name': 'Month'}
    )
    st.plotly_chart(fig)

st.dataframe(full_data)



# Ensure Connection is Closed After Execution

#conn.close()



