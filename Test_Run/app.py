import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
import plotly.express as px
import matplotlib.pyplot as plt
import seaborn as sns

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

   # --- 📈 Normalized MTA Tax (per Fare Amount) Line Plot ---
    normalized_data = filtered_data.copy()
    normalized_data = normalized_data[(normalized_data['fare_amount'] > 0) & (normalized_data['mta_tax'].notna())]

    normalized_data['mta_tax_ratio'] = normalized_data['mta_tax'] / normalized_data['fare_amount']

    mta_ratio_avg = (
        normalized_data.groupby(['year', 'month'])['mta_tax_ratio']
        .mean()
        .reset_index()
    )
    mta_ratio_avg['month_name'] = mta_ratio_avg['month'].apply(lambda m: ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][m-1])
    mta_ratio_avg['month_name'] = pd.Categorical(mta_ratio_avg['month_name'],
                                                 categories=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
                                                 ordered=True)
    mta_ratio_avg['year'] = mta_ratio_avg['year'].astype(str)

    fig_line = px.line(
        mta_ratio_avg,
        x='month_name',
        y='mta_tax_ratio',
        color='year',
        title='📈 Normalized MTA Tax (as % of Fare Amount)',
        markers=True,
        labels={'mta_tax_ratio': 'MTA Tax / Fare Amount', 'month_name': 'Month'}
    )
    st.plotly_chart(fig_line)
#Begin Passenger count

# Extract the year directly from the first 4 characters of 'pickup_date'
data = fetch_all_data()
data['year'] = data['pickup_date'].astype(str).str[:4]
data['year'] = data['year'].astype(int)

        # Filter for the years 2019, 2021, and 2023
filtered = data[data['year'].isin([2019, 2021, 2023])]

        # Drop rows with missing 'passenger_count' values
filtered = filtered.dropna(subset=['passenger_count'])

        # Create the boxplot
fig, ax = plt.subplots()
sns.boxplot(x='year', y='passenger_count', data=filtered, ax=ax)
ax.set_title("Passenger Count by Year")
ax.set_xlabel("Year")
ax.set_ylabel("Passenger Count")
        
# Display the plot in Streamlit
st.pyplot(fig)

st.dataframe(full_data)


data['pickup_datetime'] = pd.to_datetime(data['pickup_date'] + ' ' + data['pickup_time'], errors='coerce')

data['year'] = data['pickup_datetime'].dt.year
data['month'] = data['pickup_datetime'].dt.month

if data['total_time'].dtype == 'object':
    data['total_time'] = pd.to_timedelta(data['total_time'], errors='coerce')

data['total_minutes'] = data['total_time'].dt.total_seconds() / 60

duration_filtered = data[data['year'].isin([2019, 2021, 2023])]

duration_filtered = duration_filtered.dropna(subset=['total_minutes'])
duration_filtered = duration_filtered[duration_filtered['total_minutes'] > 0]

monthly_avg_duration = (
    duration_filtered.groupby(['year', 'month'])['total_minutes']
    .mean()
    .reset_index()
)

monthly_avg_duration['month_name'] = monthly_avg_duration['month'].apply(
    lambda m: ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][m-1]
)
monthly_avg_duration['month_name'] = pd.Categorical(
    monthly_avg_duration['month_name'],
    categories=['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'],
    ordered=True
)
monthly_avg_duration['year'] = monthly_avg_duration['year'].astype(str)

st.markdown("## 📈 Average Ride Duration per Month by Year")
fig = px.line(
    monthly_avg_duration,
    x='month_name',
    y='total_minutes',
    color='year',
    markers=True,
    title='📈 Average Ride Duration (in Minutes) by Month',
    labels={'total_minutes': 'Average Duration (minutes)', 'month_name': 'Month'}
)
st.plotly_chart(fig)


# --- 💵 Avg Tip per Ride by Passenger Count (Line Chart) ---
st.markdown("## 💵 Average Tip per Ride by Passenger Count (by Year)")

# Ensure passenger_count and tip_amount are clean
tip_data = filtered_data[
    (filtered_data['tip_amount'].notna()) &
    (filtered_data['tip_amount'] >= 0) &
    (filtered_data['passenger_count'].notna())
]
tip_data = tip_data[(tip_data['passenger_count'] > 0) & (tip_data['passenger_count'] <= 6)]

# Group by year and passenger count
tip_by_passenger = (
    tip_data.groupby(['year', 'passenger_count'])
    .agg(
        total_tip=('tip_amount', 'sum'),
        ride_count=('tip_amount', 'count')
    )
    .reset_index()
)

tip_by_passenger['avg_tip_per_ride'] = tip_by_passenger['total_tip'] / tip_by_passenger['ride_count']
tip_by_passenger['year'] = tip_by_passenger['year'].astype(str)

# Sort values to keep line plots clean
tip_by_passenger = tip_by_passenger.sort_values(by=['year', 'passenger_count'])

# Plot
fig_tip_line = px.line(
    tip_by_passenger,
    x='passenger_count',
    y='avg_tip_per_ride',
    color='year',
    markers=True,
    title='💵 Average Tip per Ride by Passenger Count (Grouped by Year)',
    labels={
        'passenger_count': 'Passenger Count',
        'avg_tip_per_ride': 'Avg Tip per Ride ($)',
        'year': 'Year'
    }
)
st.plotly_chart(fig_tip_line)



# Ensure Connection is Closed After Execution

#conn.close()



