-- =====================================================================
-- ANALYTICS SQL SCRIPT
-- Includes ALL KPI views, product insights, category insights,
-- customer retention, campaign performance and spend diagnostics.
-- =====================================================================

SET search_path TO marketing;

-- ===========================================================
-- 0) BASIC P&L SUMMARY
-- ===========================================================
SELECT 
    SUM(revenue) AS revenue,
    SUM(cost) AS cogs,
    SUM(spend) AS ad_spending,
    SUM(revenue - cost) AS gross_profit,
    ROUND(SUM(revenue - cost - spend)::numeric, 2) AS net_profit
FROM marketing.mv_channel_daily;


-- ===========================================================
-- 1. PRODUCT PERFORMANCE
-- ===========================================================

-- 1.1 Best Selling Products (Top 5)
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

-- 1.2 Highest Profit Products (Top 5)
SELECT
    p.item_name,
    p.category,
    SUM(fs.revenue - fs.cost) AS total_profit
FROM marketing.fact_sales fs
JOIN marketing.dim_products p ON fs.product_id = p.product_id
GROUP BY p.product_id, p.item_name, p.category
ORDER BY total_profit DESC
LIMIT 5;

-- 1.3 Highest Profit Margin (Top 5)
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


-- ===========================================================
-- 2. CHANNEL PERFORMANCE (ROAS, ROI, CTR, AOV)
-- ===========================================================
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


-- ===========================================================
-- 3. REPEAT PURCHASE DRIVERS
-- ===========================================================
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


-- ===========================================================
-- 4. CATEGORY WINNERS & LOSERS
-- ===========================================================
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


-- ===========================================================
-- 5. HERO PRODUCT VS FREE RIDER MODEL
-- ===========================================================
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
        WHEN pp.units_sold > s.avg_units AND pp.margin > s.avg_margin THEN 'HERO PRODUCT 🔥'
        WHEN pp.units_sold < s.avg_units AND pp.margin > s.avg_margin THEN 'High Potential'
        WHEN pp.units_sold > s.avg_units AND pp.margin < s.avg_margin THEN 'Volume Driver'
        ELSE 'Free Rider'
    END AS product_type
FROM product_profit pp
CROSS JOIN stats s
ORDER BY pp.revenue DESC;


-- ===========================================================
-- 6. SPEND WASTE DETECTION
-- ===========================================================
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


-- ===========================================================
-- 7. CUSTOMER RETENTION RATE
-- ===========================================================
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


-- ===========================================================
-- 8. Demographic Segmentation (Age Group × Gender)
-- ===========================================================
WITH customer_segments AS (
    SELECT
        customer_id,
        CASE 
            WHEN age < 20 THEN 'Under 20'
            WHEN age BETWEEN 20 AND 29 THEN '20–29'
            WHEN age BETWEEN 30 AND 39 THEN '30–39'
            WHEN age BETWEEN 40 AND 49 THEN '40–49'
            ELSE '50+'
        END AS age_group,
        gender
    FROM marketing.dim_customers
),
segment_sales AS (
    SELECT
        cs.age_group,
        cs.gender,
        COUNT(DISTINCT fs.customer_id) AS customers,
        SUM(fs.revenue) AS total_revenue,
        SUM(fs.quantity) AS total_units
    FROM marketing.fact_sales fs
    JOIN customer_segments cs USING (customer_id)
    GROUP BY cs.age_group, cs.gender
),
totals AS (
    SELECT
        SUM(customers) AS all_customers,
        SUM(total_revenue) AS all_revenue,
        SUM(total_units) AS all_units
    FROM segment_sales
)
SELECT
    s.*,
    ROUND(s.customers::numeric / t.all_customers * 100, 2) AS pct_customers,
    ROUND(s.total_revenue::numeric / t.all_revenue::numeric * 100, 2) AS pct_revenue
FROM segment_sales s CROSS JOIN totals t
ORDER BY pct_revenue DESC;


-- ===========================================================
-- 9. Frequency Segmentation (Heavy Buyers vs One-Time Buyers)
-- ===========================================================
WITH customer_freq AS (
    SELECT
        customer_id,
        COUNT(*) AS purchase_count
    FROM marketing.fact_sales
    GROUP BY customer_id
),
freq_segment AS (
    SELECT 
        customer_id,
        CASE 
            WHEN purchase_count >= 10 THEN 'Whales (10+ orders)'
            WHEN purchase_count >= 5 THEN 'Heavy Buyers (5–9)'
            WHEN purchase_count >= 2 THEN 'Repeat Buyers (2–4)'
            ELSE 'One-Time Buyers'
        END AS segment
    FROM customer_freq
),
segment_sales AS (
    SELECT
        segment,
        COUNT(*) AS customers,
        SUM(fs.revenue) AS total_revenue,
        SUM(fs.quantity) AS total_units
    FROM freq_segment f
    JOIN marketing.fact_sales fs USING (customer_id)
    GROUP BY segment
),
totals AS (
    SELECT
        SUM(customers) AS all_customers,
        SUM(total_revenue) AS all_revenue,
        SUM(total_units) AS all_units
    FROM segment_sales
)
SELECT
    s.*,
    ROUND(s.customers::numeric / t.all_customers * 100, 2) AS pct_customers,
    ROUND(s.total_revenue::numeric / t.all_revenue::numeric * 100, 2) AS pct_revenue,
    ROUND(s.total_units::numeric / t.all_units * 100, 2) AS pct_units
FROM segment_sales s CROSS JOIN totals t
ORDER BY pct_revenue DESC;
