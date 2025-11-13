-- ==========================================
-- FULL ETL PIPELINE FOR MARKETING ANALYTICS
-- Works locally using \copy (no permission issues)
-- ==========================================

-- ============================
-- 1️⃣ Clean rebuild schema
-- ============================
DROP TABLE IF EXISTS fact_spend;
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_campaigns;
DROP TABLE IF EXISTS dim_products;
DROP TABLE IF EXISTS dim_customers;
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date (
  date_id INTEGER PRIMARY KEY,
  date DATE,
  day INTEGER,
  week INTEGER,
  month INTEGER,
  year INTEGER,
  season TEXT
);

CREATE TABLE dim_customers (
  customer_id VARCHAR(10) PRIMARY KEY,
  age INTEGER,
  gender TEXT,
  location TEXT,
  subscription_status TEXT,
  frequency_band TEXT
);

CREATE TABLE dim_products (
  product_id INTEGER PRIMARY KEY,
  item_name TEXT,
  category TEXT,
  base_price REAL,
  cost_price REAL
);

CREATE TABLE dim_campaigns (
  campaign_id INTEGER PRIMARY KEY,
  campaign_name TEXT,
  channel TEXT
);

CREATE TABLE fact_sales (
  sale_id SERIAL PRIMARY KEY,
  date_id INTEGER,
  customer_id VARCHAR(10),
  product_id INTEGER,
  campaign_id INTEGER,
  quantity INTEGER,
  revenue REAL,
  cost REAL,
  shipping_type TEXT,
  payment_method TEXT,
  prev_purchases INTEGER,
  FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY(customer_id) REFERENCES dim_customers(customer_id),
  FOREIGN KEY(product_id) REFERENCES dim_products(product_id),
  FOREIGN KEY(campaign_id) REFERENCES dim_campaigns(campaign_id)
);

CREATE TABLE fact_spend (
  spend_id SERIAL PRIMARY KEY,
  date_id INTEGER,
  campaign_id INTEGER,
  spend REAL,
  impressions INTEGER,
  clicks INTEGER,
  observed_ctr REAL,
  FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY(campaign_id) REFERENCES dim_campaigns(campaign_id)
);

-- ============================
-- 2️⃣ Load CSVs using \copy
-- ============================
\echo '📦 Loading CSV data into tables...'

TRUNCATE dim_campaigns, dim_products, dim_customers, dim_date, fact_sales, fact_spend;

\copy dim_campaigns FROM '../data/raw/campaigns_details.csv' WITH (FORMAT csv, HEADER true);
\copy dim_products FROM './data/raw/promotion_reference.csv' WITH (FORMAT csv, HEADER true);
\copy fact_spend FROM './data/raw/channel_spend_daily_campaign.csv' WITH (FORMAT csv, HEADER true);
\copy fact_sales FROM './data/raw/ecom_mens_streetwear_10000.csv' WITH (FORMAT csv, HEADER true);

\echo '✅ CSV loading complete!'

-- ============================
-- 3️⃣ Transformations
-- ============================

\echo '⚙️ Running transformations...'

-- Build dim_date from fact_sales
INSERT INTO dim_date (date_id, date, day, week, month, year, season)
SELECT DISTINCT
  EXTRACT(EPOCH FROM fs.date::timestamp)::INT AS date_id,
  fs.date::date,
  EXTRACT(DAY FROM fs.date),
  EXTRACT(WEEK FROM fs.date),
  EXTRACT(MONTH FROM fs.date),
  EXTRACT(YEAR FROM fs.date),
  CASE
    WHEN EXTRACT(MONTH FROM fs.date) BETWEEN 3 AND 5 THEN 'Summer'
    WHEN EXTRACT(MONTH FROM fs.date) BETWEEN 6 AND 9 THEN 'Rainy'
    ELSE 'Cool'
  END
FROM fact_sales fs
ON CONFLICT (date_id) DO NOTHING;

-- Add dummy customers if not already created
INSERT INTO dim_customers (customer_id, age, gender, location, subscription_status, frequency_band)
SELECT DISTINCT
  customer_id, age, gender, location, subscription_status, frequency_band
FROM fact_sales
ON CONFLICT (customer_id) DO NOTHING;

\echo '✅ Transformation complete!'

-- ============================
-- 4️⃣ Create KPI Views
-- ============================

DROP VIEW IF EXISTS mv_channel_daily;
DROP VIEW IF EXISTS mv_kpi_channel;
DROP VIEW IF EXISTS mv_kpi_campaign;

-- Daily revenue/spend summary per channel
CREATE VIEW mv_channel_daily AS
SELECT 
    d.date,
    c.channel,
    SUM(fs.revenue) AS revenue,
    SUM(fs.cost) AS cost,
    COALESCE(SUM(sp.spend), 0) AS spend
FROM fact_sales fs
LEFT JOIN dim_campaigns c ON fs.campaign_id = c.campaign_id
LEFT JOIN dim_date d ON fs.date_id = d.date_id
LEFT JOIN fact_spend sp ON sp.date_id = fs.date_id AND sp.campaign_id = fs.campaign_id
GROUP BY d.date, c.channel;

-- Channel-level KPIs
CREATE VIEW mv_kpi_channel AS
SELECT 
    channel,
    SUM(revenue) AS revenue,
    SUM(cost) AS cost,
    SUM(spend) AS spend,
    CASE WHEN SUM(spend)=0 THEN NULL ELSE SUM(revenue)/SUM(spend) END AS roas,
    CASE WHEN SUM(spend)=0 THEN NULL ELSE (SUM(revenue)-SUM(spend))/SUM(spend) END AS roi,
    CASE WHEN SUM(spend)=0 THEN NULL ELSE (SUM(revenue)-SUM(cost))/SUM(spend) END AS profit_roas,
    CASE WHEN SUM(spend)=0 THEN NULL ELSE (SUM(revenue)-SUM(cost)-SUM(spend))/SUM(spend) END AS profit_roi
FROM mv_channel_daily
GROUP BY channel;

-- Campaign-level KPIs
CREATE VIEW mv_kpi_campaign AS
SELECT 
    c.channel,
    c.campaign_name,
    fs.campaign_id,
    SUM(fs.revenue) AS revenue,
    SUM(fs.cost) AS cost,
    COALESCE(SUM(sp.spend), 0) AS spend,
    CASE WHEN COALESCE(SUM(sp.spend),0)=0 THEN NULL 
         ELSE SUM(fs.revenue)/SUM(sp.spend) END AS roas,
    CASE WHEN COALESCE(SUM(sp.spend),0)=0 THEN NULL 
         ELSE (SUM(fs.revenue)-SUM(fs.cost))/SUM(sp.spend) END AS profit_roas,
    CASE WHEN COALESCE(SUM(sp.spend),0)=0 THEN NULL 
         ELSE (SUM(fs.revenue)-SUM(fs.cost)-SUM(sp.spend))/SUM(spend) END AS profit_roi
FROM fact_sales fs
LEFT JOIN dim_campaigns c ON fs.campaign_id = c.campaign_id
LEFT JOIN fact_spend sp ON sp.campaign_id = fs.campaign_id AND sp.date_id = fs.date_id
GROUP BY c.channel, c.campaign_name, fs.campaign_id;

\echo '🎯 All KPI views created successfully!'