import os
from sqlalchemy import create_engine, text

print("📝 Welcome! Let's add your quote to the PostgreSQL database.")
quote = input("Enter your favorite quote: ")
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)

with engine.connect() as conn:
    conn.execute(text("CREATE TABLE IF NOT EXISTS quotes (id SERIAL PRIMARY KEY, message TEXT, timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"))
    conn.execute(text("INSERT INTO quotes (message) VALUES (:message);"), {"message": quote})
    conn.commit()
    result = conn.execute(text("SELECT * FROM quotes"))
    print("\n📜 All Quotes in the PostgreSQL Database:")
    for row in result:
        print(f"{row.id}: {row.message}   added on {row.timestamp}")

#Now we will setup a streamlit thingy