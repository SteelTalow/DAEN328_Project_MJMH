import pandas as pd
from sqlalchemy import create_engine

def load_postgress(taxi_data):
    #working here
    print()



#Code below will upload without first needing to make schema. Investigate whether that is what we want

#make sure to figure out exactly how/what the connection paramaters will be

# Load your CSV
df = pd.read_csv('your_file.csv')

# Create DB connection (edit user/pass/db/host as needed)
engine = create_engine('postgresql://username:password@localhost:5432/your_db')

# Send to Postgres (will auto-create table)
df.to_sql('your_table_name', engine, if_exists='replace', index=False)
