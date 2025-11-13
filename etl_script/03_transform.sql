
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