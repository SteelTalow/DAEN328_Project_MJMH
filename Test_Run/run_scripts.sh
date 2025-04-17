#!/bin/bash

echo "⏳ Waiting for Postgres to be ready..."
until pg_isready -h db -p 5432 -U postgres; do
  sleep 1
done
echo "✅ Postgres is ready!"

#python apple.py

python Extract.py

python transform.py

python load.py

streamlit run app.py --server.port=8501