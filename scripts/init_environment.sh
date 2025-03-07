#!/bin/bash

# Initialize environment script
echo "Initializing environment..."

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
until PGPASSWORD=mysecretpassword psql -h postgres -U postgres -c '\q'; do
  echo "PostgreSQL is unavailable - sleeping"
  sleep 1
done

echo "PostgreSQL is up - executing command"

# Create the sales database if it doesn't exist
PGPASSWORD=mysecretpassword psql -h postgres -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'sales'" | grep -q 1 || PGPASSWORD=mysecretpassword psql -h postgres -U postgres -c "CREATE DATABASE sales"

# Run the data ingestion script
echo "Running data ingestion script..."
python /scripts/ingest_sales_data.py

# Initialize dbt
echo "Initializing dbt..."
cd /dbt && dbt debug --profiles-dir /dbt/profiles

echo "Environment initialization completed!" 