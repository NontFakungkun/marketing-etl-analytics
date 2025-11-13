#!/bin/bash

DB_NAME="marketingdb"
DB_USER="postgres"
DB_HOST="localhost"
DB_PORT="5432"

echo "Checking if database '$DB_NAME' exists..."
if ! psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
  echo "Database not found. Creating '$DB_NAME'..."
  createdb -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" "$DB_NAME"
else
  echo "Database '$DB_NAME' already exists."
fi

echo "-------------------------------------"
echo "[1/4] Creating schema & tables..."
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -f 01_schema.sql

echo "-------------------------------------"
echo "[2/4] Loading CSV data into staging..."
python3 02_load_csv.py

echo "-------------------------------------"
echo "[3/4] Running transformations..."
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -f 03_transform.sql

# --- 5️⃣ Create KPI views
echo "-------------------------------------"
echo "[4/4] Building KPI views..."
psql -U "$DB_USER" -h "$DB_HOST" -p "$DB_PORT" -d "$DB_NAME" -f 04_views.sql


echo "-------------------------------------"
echo "ETL Pipeline completed successfully for '$DB_NAME'!"