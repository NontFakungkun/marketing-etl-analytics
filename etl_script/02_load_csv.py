#!/usr/bin/env python3
"""
ETL Step 2 (Python version)
- Loads CSVs into PostgreSQL staging schema
- Calls SQL Step 3 and Step 4 to transform + create views
"""

import pandas as pd
from sqlalchemy import create_engine, text

# === CONFIG ===
DB_URL = "postgresql+psycopg2://postgres:password@localhost:5432/marketingdb"

CSV_PATHS = {
    "transactions": "../data/raw/ecom_mens_streetwear_10000.csv",
    "campaigns": "../data/raw/campaigns_details.csv",
    "spend": "../data/raw/channel_spend_daily_campaign.csv",
    "promo": "../data/raw/promotion_reference.csv",
}

# === CONNECT ===
engine = create_engine(DB_URL)

# === LOAD STAGING TABLES ===
print("Loading CSVs into staging schema...")

df_tx = pd.read_csv(CSV_PATHS["transactions"])
df_spend = pd.read_csv(CSV_PATHS["spend"])
df_promo = pd.read_csv(CSV_PATHS["promo"])

with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    conn.execute(text("DROP TABLE IF EXISTS staging.stg_transactions, staging.stg_channel_spend_daily, staging.stg_promotion_reference CASCADE;"))

# Upload to Postgres
df_tx.to_sql("stg_transactions", engine, schema="staging", if_exists="replace", index=False)
df_spend.to_sql("stg_channel_spend_daily", engine, schema="staging", if_exists="replace", index=False)
df_promo.to_sql("stg_promotion_reference", engine, schema="staging", if_exists="replace", index=False)

print("CSVs loaded to staging.")

print("Step 2 completed successfully.")
