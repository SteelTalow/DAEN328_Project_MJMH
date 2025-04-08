import os
from sqlalchemy import create_engine, text

print("📝 Welcome! Let's add your quote to the PostgreSQL database.")
quote = "Here is my quote"
db_url = os.getenv("DATABASE_URL")
engine = create_engine(db_url)

with engine.begin() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS quotes (
            id SERIAL PRIMARY KEY,
            message TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """))
    conn.execute(text("INSERT INTO quotes (message) VALUES (:message);"), {"message": quote})

