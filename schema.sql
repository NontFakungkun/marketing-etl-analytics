CREATE TABLE IF NOT EXISTS dim_date (
  date_id INTEGER PRIMARY KEY,
  date DATE,
  day INTEGER,
  week INTEGER,
  month INTEGER,
  year INTEGER,
  season TEXT
);

CREATE TABLE IF NOT EXISTS dim_customers (
  customer_id TEXT PRIMARY KEY,
  age INTEGER,
  gender TEXT,
  location TEXT,
  frequency_band TEXT
);

CREATE TABLE IF NOT EXISTS dim_products (
  product_id INTEGER PRIMARY KEY,
  item_name TEXT,
  category TEXT,
  base_price NUMERIC
);

CREATE TABLE IF NOT EXISTS dim_promotions (
  promo_code TEXT PRIMARY KEY,
  discount_pct NUMERIC
);

CREATE TABLE IF NOT EXISTS dim_campaigns (
  campaign_id INTEGER PRIMARY KEY,
  campaign_name TEXT,
  channel TEXT,
  promo_code TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
  sale_id INTEGER PRIMARY KEY,
  date_id INTEGER,
  customer_id TEXT,
  product_id INTEGER,
  campaign_id INTEGER,
  quantity INTEGER,
  revenue NUMERIC,
  cost NUMERIC,
  shipping_type TEXT,
  payment_method TEXT,
  prev_purchases INTEGER,
  FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY(customer_id) REFERENCES dim_customers(customer_id),
  FOREIGN KEY(product_id) REFERENCES dim_products(product_id),
  FOREIGN KEY(campaign_id) REFERENCES dim_campaigns(campaign_id)
);

CREATE TABLE IF NOT EXISTS fact_spend (
  spend_id INTEGER PRIMARY KEY,
  date_id INTEGER,
  campaign_id INTEGER,
  spend NUMERIC,
  impressions INTEGER,
  clicks INTEGER,
  observed_ctr NUMERIC,
  FOREIGN KEY(date_id) REFERENCES dim_date(date_id),
  FOREIGN KEY(campaign_id) REFERENCES dim_campaigns(campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON fact_sales(date_id);
CREATE INDEX IF NOT EXISTS idx_sales_camp ON fact_sales(campaign_id);
CREATE INDEX IF NOT EXISTS idx_spend_date_camp ON fact_spend(date_id, campaign_id);
