# ⭐ End-to-End Marketing Analytics System

## 🧩 **PROJECT OVERVIEW — Marketing Analytics Engine (Streetwear E-commerce)**

### 🎯 **Goal**

This project simulates and builds a complete **Marketing Analytics Engine** for a fictional streetwear e-commerce brand selling men’s apparel.

It unifies **sales**, **ad spend**, **campaign metadata**, and **promotion data** into a centralized PostgreSQL warehouse — fully automated via an ETL pipeline.

### 📅 **Analysis Period** [**2024-11-01 → 2025-10-31**]

---

# 🛠️ **PHASE 1 — Business Requirements & Data Questions**

Before building the pipeline, the project defines meaningful business questions that guide schema design, ETL rules, and KPI definition.

## **1. Channel & Campaign Performance**

* Which channel delivers the best **revenue, ROAS, ROI, profit**?
* Which campaigns overspend (high spend × low clicks × low revenue)?
* Where should we allocate more budget next month?

## **2. Product & Category Intelligence**

* What are the **best-selling** and **highest-margin** products?
* Which products are **repeat-purchase drivers** (gateway items)?
* Which categories are **winners** and **losers**?

## **3. Customer Behavior**

* What is our **retention rate**?
* Which products/categories increase customer loyalty?
* Which customer segments buy the most?

---

# 📦 **Data Assets**

All raw data stored in `/data/raw/`:

| File Name                          | Description                 |
| ---------------------------------- | --------------------------- |
| `ecom_mens_streetwear_10000.csv`   | Sales transactions          |
| `channel_spend_daily_campaign.csv` | Daily spend per campaign    |
| `campaigns_details.csv`            | Campaign ID-to-name mapping |
| `promotion_reference.csv`          | Promotion metadata          |

---

# 🧩 **Star Schema Overview**

### **Dimension Tables**

* `dim_products`
* `dim_campaigns`
* `dim_customers`
* `dim_date`

### **Fact Tables**

* `fact_sales`
* `fact_spend`

---

# ⚙️ **PHASE 2 — Data Modeling & ETL Automation**
```bash
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
```sql
### ETL Execution (Shell Script)
This phase builds the entire backend foundation of the project. The pipeline set up schema → loads raw CSVs → transforms them → and generates clean, analysis-ready tables and KPI views.
```bash
bash run_etl.sh
```

### **The 4 ETL Steps**

### **1️⃣ Create Schema & Tables**

`01_schema.sql`
Builds all fact + dimension tables with proper PK/FK definitions.

---

### **2️⃣ Load Raw CSVs into Staging**

`02_load_csv.py`
Uses Python + SQLAlchemy to load data safely (avoids `COPY` permission errors).

```python
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
```
---

### **3️⃣ Transform Data into Facts & Dimensions**

`03_transform.sql`

* Builds `dim_*` tables
* Populates `fact_sales` and `fact_spend` correctly

```
-- 03_transform_step.sql: Transform staging data -> marketing schema

-- 1. Dates
INSERT INTO marketing.dim_date (date_id, date, day, week, month, year)
SELECT DISTINCT 
    EXTRACT(EPOCH FROM to_date("Transaction Date", 'MM/DD/YYYY'))::BIGINT AS date_id,
    to_date("Transaction Date", 'MM/DD/YYYY') AS date,
    EXTRACT(DAY FROM to_date("Transaction Date", 'MM/DD/YYYY')) AS day,
    EXTRACT(WEEK FROM to_date("Transaction Date", 'MM/DD/YYYY')) AS week,
    EXTRACT(MONTH FROM to_date("Transaction Date", 'MM/DD/YYYY')) AS month,
    EXTRACT(YEAR FROM to_date("Transaction Date", 'MM/DD/YYYY')) AS year
FROM staging.stg_transactions
ON CONFLICT (date_id) DO NOTHING;


-- 2. Customers
INSERT INTO marketing.dim_customers (customer_id, age, gender, location, subscription_status)
SELECT DISTINCT 
    "Customer ID", "Age", "Gender", "Location", "Subscription Status"
FROM staging.stg_transactions
ON CONFLICT (customer_id) DO NOTHING;

-- 3. Products
INSERT INTO marketing.dim_products (item_name, category, base_price, cost_price)
SELECT DISTINCT "Item Purchased", "Category", ROUND(SUM("Purchase Amount (THB)")/sum("Quantity"),0) AS unit_price_thb, ROUND(SUM("Cost Price (THB)")/sum("Quantity"),0)
FROM staging.stg_transactions
GROUP BY "Item Purchased", "Category";

-- 4. Campaigns
INSERT INTO marketing.dim_campaigns (campaign_name, channel)
SELECT DISTINCT "Campaign Name", REGEXP_SUBSTR("Campaign Name", '^[^0-9]+')
FROM staging.stg_transactions
order by "Campaign Name";

-- 5. Sales Fact
INSERT INTO marketing.fact_sales (date_id, customer_id, product_id, campaign_id, quantity, revenue, cost, shipping_type, payment_method, prev_purchases)
SELECT 
    d.date_id,
    s."Customer ID",
    p.product_id,
    c.campaign_id,
    s."Quantity",
    s."Purchase Amount (THB)"::numeric,
    s."Cost Price (THB)"::numeric,
    s."Shipping Type",
    s."Payment Method",
    s."Previous Purchases"
FROM staging.stg_transactions s
LEFT JOIN marketing.dim_date d ON to_date(s."Transaction Date", 'MM/DD/YYYY') = d.date
LEFT JOIN marketing.dim_products p ON s."Item Purchased" = p.item_name
LEFT JOIN marketing.dim_campaigns c ON s."Campaign Name" = c.campaign_name;

-- 6. Spend Fact
INSERT INTO marketing.fact_spend (date_id, campaign_id, spend, impressions, clicks, observed_ctr)
SELECT 
    d.date_id,
    c.campaign_id,
    sp."Spending"::numeric,
    sp."Impressions",
    sp."Clicks",
    sp."Observed CTR"::numeric
FROM staging.stg_channel_spend_daily sp
LEFT JOIN marketing.dim_date d ON to_date(sp."Date", 'MM/DD/YYYY') = d.date
LEFT JOIN marketing.dim_campaigns c ON sp."Campaign Name" = c.campaign_name;
```

---

### **4️⃣ Build KPI Views**

`04_views.sql`
* Creates `mv_channel_daily`, `mv_kpi_channel`, `mv_kpi_campaign`
* Calculates ROAS / ROI, Profit ROAS / Profit ROI, CTR, AOV, Daily performance

---

# ⏰ **ETL Scheduling (Daily Automation)**

A cron job refreshes the warehouse every midnight:

```bash
crontab -e
0 0 * * * /path/to/project/run_etl.sh >> /path/to/project/logs/etl.log 2>&1
```

---

# 📊 **PHASE 3 — Core Analytics & Metrics**

This phase transforms the marketing warehouse into actionable insights using SQL.

---

## **1. Product Intelligence**

### **Top 5 Best-Selling Products**

```sql
SELECT
    p.product_id,
    p.item_name,
    p.category,
    SUM(fs.quantity) AS total_units_sold
FROM marketing.fact_sales fs
JOIN marketing.dim_products p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.item_name, p.category
ORDER BY total_units_sold DESC
LIMIT 5;
```

### **Top 5 Highest-Profit Products**

```sql
SELECT
    p.item_name,
    p.category,
    SUM(fs.revenue - fs.cost) AS total_profit
FROM marketing.fact_sales fs
JOIN marketing.dim_products p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.item_name, p.category
ORDER BY total_profit DESC
LIMIT 5;
```

### **Top 5 Highest-Margin Products**

```sql
SELECT
    p.product_id,
    p.item_name,
    p.category,
    SUM(fs.revenue) AS total_revenue,
    SUM(fs.cost) AS total_cost,
    ROUND((SUM(fs.revenue) - SUM(fs.cost))::numeric / SUM(fs.revenue)::numeric * 100, 2)
        AS profit_margin_pct
FROM marketing.fact_sales fs
JOIN marketing.dim_products p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.item_name, p.category
HAVING SUM(fs.revenue) > 0
ORDER BY profit_margin_pct DESC
LIMIT 5;
```

### **Hero vs Free Rider (Volume vs Margin Model)**

```sql
WITH product_profit AS (
    SELECT
        p.product_id,
        p.item_name,
        SUM(fs.quantity) AS units_sold,
        SUM(fs.revenue) AS revenue,
        SUM(fs.revenue - fs.cost) AS profit,
        ROUND(AVG((fs.revenue - fs.cost) / NULLIF(fs.revenue,0))::numeric, 3) AS margin
    FROM marketing.fact_sales fs
    JOIN marketing.dim_products p ON fs.product_id = p.product_id
    GROUP BY p.product_id, p.item_name
),
stats AS (
    SELECT
        AVG(units_sold) AS avg_units,
        AVG(margin) AS avg_margin
    FROM product_profit
)
SELECT
    pp.*,
    CASE
        WHEN pp.units_sold > s.avg_units AND pp.margin > s.avg_margin THEN 'HERO PRODUCT'
        WHEN pp.units_sold < s.avg_units AND pp.margin > s.avg_margin THEN 'High Potential'
        WHEN pp.units_sold > s.avg_units AND pp.margin < s.avg_margin THEN 'Volume Driver'
        ELSE 'Free Rider'
    END AS product_type
FROM product_profit pp
CROSS JOIN stats s
ORDER BY pp.revenue DESC;
```

---

## **2. Customer Behavior**

### **Repeat Purchase Drivers (Gateway Products)**

```sql
WITH first_product AS (
    SELECT customer_id, product_id, MIN(date_id) AS first_date
    FROM marketing.fact_sales
    GROUP BY customer_id, product_id
),
customer_repeat_flag AS (
    SELECT
        fs.customer_id,
        fs.product_id,
        CASE WHEN fs.date_id > fp.first_date THEN 1 ELSE 0 END AS is_repeat
    FROM marketing.fact_sales fs
    JOIN first_product fp USING (customer_id, product_id)
)
SELECT
    p.item_name,
    COUNT(*) FILTER (WHERE is_repeat = 1) AS repeat_buyers,
    COUNT(*) AS total_buyers,
    ROUND(
        COUNT(*) FILTER (WHERE is_repeat = 1)::numeric /
        NULLIF(COUNT(*),0), 3
    ) AS repeat_rate
FROM customer_repeat_flag cr
JOIN marketing.dim_products p ON cr.product_id = p.product_id
GROUP BY p.item_name
ORDER BY repeat_rate DESC
LIMIT 10;
```

### **Customer Retention Analysis**

```sql
WITH first_purchase AS (
  SELECT customer_id, MIN(date_id) AS first_purchase_date
  FROM marketing.fact_sales
  GROUP BY customer_id
)
SELECT
  COUNT(*) FILTER (WHERE fs.date_id > fp.first_purchase_date) AS returning_customers,
  COUNT(*) AS total_customers,
  ROUND(
    COUNT(*) FILTER (WHERE fs.date_id > fp.first_purchase_date)::numeric /
    NULLIF(COUNT(*),0), 3
  ) AS retention_rate
FROM marketing.fact_sales fs
JOIN first_purchase fp USING (customer_id);
```

### **Category Winner / Loser Model**

```sql
WITH sales_enriched AS (
    SELECT fs.*, p.category
    FROM marketing.fact_sales fs
    JOIN marketing.dim_products p ON fs.product_id = p.product_id
),
customer_loyalty AS (
    SELECT customer_id, COUNT(*) AS purchase_count
    FROM marketing.fact_sales
    GROUP BY customer_id
),
category_loyalty AS (
    SELECT
        s.category,
        COUNT(*) FILTER (WHERE cl.purchase_count > 1) AS repeat_customers,
        COUNT(*) AS total_customers
    FROM sales_enriched s
    JOIN customer_loyalty cl USING (customer_id)
    GROUP BY s.category
)
SELECT
    s.category,
    SUM(s.revenue) AS revenue,
    SUM(s.revenue - s.cost) AS profit,
    ROUND(AVG((s.revenue - s.cost) / NULLIF(s.revenue,0))::numeric, 3) AS avg_margin,
    repeat_customers,
    total_customers,
    ROUND(repeat_customers::numeric / NULLIF(total_customers,0), 3) AS loyalty_rate
FROM sales_enriched s
JOIN category_loyalty USING (category)
GROUP BY s.category, repeat_customers, total_customers
ORDER BY revenue DESC;
```

---

## **3. Channel & Campaign Performance**

### **Profitability Table (ROAS, ROI, CTR, AOV, Profitability)**

```sql
SELECT
  channel,
  SUM(revenue) AS revenue,
  SUM(cost) AS cost,
  SUM(revenue - cost) AS gross_profit,
  SUM(spend) AS spend,
  ROUND(SUM(revenue - cost - spend)::numeric,2) AS net_profit,

  ROUND(SUM(revenue)::numeric / SUM(orders), 2) AS aov,

  ROUND(SUM(clicks)::numeric / SUM(impressions), 4) AS ctr,

  ROUND(SUM(revenue)::numeric / SUM(spend), 2) AS roas,

  ROUND((SUM(revenue) - SUM(cost))::numeric / SUM(spend), 2) AS profit_roas,

  ROUND((SUM(revenue) - SUM(spend))::numeric / SUM(spend), 2) AS roi,

  ROUND((SUM(revenue) - SUM(cost) - SUM(spend))::numeric / SUM(spend), 2) AS profit_roi
FROM marketing.mv_channel_daily
GROUP BY channel
ORDER BY net_profit DESC;
```

### **Spend Waste Detection**

```sql
SELECT
  c.campaign_name,
  c.channel,
  SUM(sp.spend) AS spend,
  SUM(sp.clicks) AS clicks,
  SUM(fs.revenue) AS revenue
FROM marketing.fact_spend sp
LEFT JOIN marketing.fact_sales fs 
       ON sp.date_id = fs.date_id 
      AND sp.campaign_id = fs.campaign_id
JOIN marketing.dim_campaigns c ON c.campaign_id = sp.campaign_id
GROUP BY c.campaign_name, c.channel
HAVING SUM(sp.spend) > 5000
   AND SUM(sp.clicks) < 100
   AND SUM(fs.revenue) < 3000
ORDER BY spend DESC;
```

---

## **4. Product–Campaign Matching**

### **Which campaign sells which products**

```sql
SELECT
  p.item_name,
  c.campaign_name,
  SUM(fs.quantity) AS units_sold,
  SUM(fs.revenue) AS revenue
FROM marketing.fact_sales fs
JOIN marketing.dim_products p ON fs.product_id = p.product_id
JOIN marketing.dim_campaigns c ON fs.campaign_id = c.campaign_id
GROUP BY p.item_name, c.campaign_name
ORDER BY revenue DESC
LIMIT 10;
```

### **AOV by Campaign**

```sql
SELECT
  c.campaign_name,
  ROUND(SUM(fs.revenue)::numeric / COUNT(fs.sale_id), 2) AS aov
FROM marketing.fact_sales fs
JOIN marketing.dim_campaigns c ON fs.campaign_id = c.campaign_id
GROUP BY c.campaign_name
ORDER BY aov DESC;
```

---

# 🏹 **PHASE 4 — Business Decisions**

### **1. Product Strategy**

* Boost **hero products** through ads & visibility
* Fix or remove **weak, low-margin** products
* Expand **high-margin** items (bundles, upsells)

### **2. Customer Strategy**

* Use gateway products for **CRM reactivation**
* Improve low-loyalty categories
* Apply **personalized recommendations**

### **3. Channel & Campaign Strategy**

* Reallocate budget to profitable channels
* Pause wasteful spenders
* Improve creatives & landing pages based on CTR/AOV

### **4. Product–Campaign Matching**

* Assign items to strongest channels (e.g. paid search for high AOV)
* Scale winning combinations
* Segment CRM audiences by campaign behavior

---

<p align="center">
  <h2 align="center">🚀 <b>This project is still growing!</b></h2>
  <p align="center">More analytics layers, visual dashboards, and automated insights are on the way.</p>
  <p align="center"><b>Sit tight for the next update 👀🔥</b></p>
</p>
