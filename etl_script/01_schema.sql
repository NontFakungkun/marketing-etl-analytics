
-- 01_schema.sql: Create schemas, staging tables, and core star schema (PostgreSQL)
DROP SCHEMA IF EXISTS staging CASCADE;
DROP SCHEMA IF EXISTS marketing CASCADE;

CREATE SCHEMA staging;
CREATE SCHEMA marketing;

-- ---------------------
-- Staging Tables
-- ---------------------
CREATE TABLE staging.stg_transactions (
    "Transaction Date" DATE,
    "Customer ID" VARCHAR(10),
    "Age" INT,
    "Gender" TEXT,
    "Item Purchased" TEXT,
    "Category" TEXT,
    "Purchase Amount (THB)" FLOAT,
    "Cost Price (THB)" FLOAT,
    "Location" TEXT,
    "Season" TEXT,
    "Subscription Status" TEXT,
    "Shipping Type" TEXT,
    "Payment Method" TEXT,
    "Previous Purchases" INT,
    "Frequency of Purchases" TEXT,
    "Campaign Name" TEXT
);

CREATE TABLE staging.stg_campaigns_details (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name TEXT,
    channel TEXT,
    promo_code TEXT,
    start_date DATE,
    end_date DATE
);

CREATE TABLE staging.stg_channel_spend_daily (
    date DATE,
    campaign_id INT,
    spend_thb FLOAT,
    impressions INT,
    clicks INT,
    observed_ctr FLOAT
);

CREATE TABLE staging.stg_promotion_reference (
    promo_code TEXT,
    description TEXT
);

-- ---------------------
-- Core Star Schema (marketing)
-- ---------------------
CREATE TABLE marketing.dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE,
    day INT,
    week INT,
    month INT,
    year INT,
    season TEXT
);

CREATE TABLE marketing.dim_customers (
    customer_id VARCHAR(10) PRIMARY KEY,
    age INT,
    gender TEXT,
    location TEXT,
    subscription_status TEXT,
    frequency_band TEXT
);

CREATE TABLE marketing.dim_products (
    product_id SERIAL PRIMARY KEY,
    item_name TEXT,
    category TEXT,
    base_price FLOAT,
    cost_price FLOAT
);

CREATE TABLE marketing.dim_campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name TEXT,
    channel TEXT
);

CREATE TABLE marketing.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES marketing.dim_date(date_id),
    customer_id VARCHAR(10) REFERENCES marketing.dim_customers(customer_id),
    product_id INT REFERENCES marketing.dim_products(product_id),
    campaign_id INT REFERENCES marketing.dim_campaigns(campaign_id),
    quantity INT,
    revenue FLOAT,
    cost FLOAT,
    shipping_type TEXT,
    payment_method TEXT,
    prev_purchases INT
);

CREATE TABLE marketing.fact_spend (
    spend_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES marketing.dim_date(date_id),
    campaign_id INT REFERENCES marketing.dim_campaigns(campaign_id),
    spend FLOAT,
    impressions INT,
    clicks INT,
    observed_ctr FLOAT
);
