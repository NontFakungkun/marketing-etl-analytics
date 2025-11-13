-- ======================================
-- STEP 4: Drop & Recreate KPI Views
-- Target schema: marketing
-- ======================================

SET search_path TO marketing;

-- Drop views (safe order)
DROP VIEW IF EXISTS mv_kpi_channel;
DROP VIEW IF EXISTS mv_kpi_campaign;
DROP VIEW IF EXISTS mv_channel_daily;

-- =====================================================
-- 1) Daily Channel Summary View
-- =====================================================
CREATE OR REPLACE VIEW mv_channel_daily AS
WITH fs_agg AS (
  SELECT date_id, campaign_id,
         SUM(revenue) AS revenue,
         SUM(cost) AS cost,
         COUNT(DISTINCT sale_id) AS orders
  FROM marketing.fact_sales
  GROUP BY date_id, campaign_id
),
sp_agg AS (
  SELECT date_id, campaign_id,
         SUM(spend) AS spend,
         SUM(clicks) AS clicks,
         SUM(impressions) AS impressions
  FROM marketing.fact_spend
  GROUP BY date_id, campaign_id
),
merged AS (
  SELECT
    COALESCE(fs.date_id, sp.date_id) AS date_id,
    COALESCE(fs.campaign_id, sp.campaign_id) AS campaign_id,
    COALESCE(fs.revenue,0) AS revenue,
    COALESCE(fs.cost,0) AS cost,
    COALESCE(fs.orders,0) AS orders,
    COALESCE(sp.spend,0) AS spend,
    COALESCE(sp.clicks,0) AS clicks,
    COALESCE(sp.impressions,0) AS impressions
  FROM fs_agg fs
  FULL OUTER JOIN sp_agg sp
    ON fs.date_id = sp.date_id
   AND fs.campaign_id = sp.campaign_id
)
SELECT
  d.date,
  c.channel,
  SUM(revenue) AS revenue,
  SUM(cost) AS cost,
  ROUND(SUM(spend)::numeric,2) AS spend,
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
  ROUND(
    (SUM(clicks) / NULLIF(SUM(impressions),0))
  , 6) AS ctr,
  SUM(orders) AS orders
FROM merged m
LEFT JOIN marketing.dim_campaigns c ON m.campaign_id = c.campaign_id
JOIN marketing.dim_date d ON m.date_id = d.date_id
GROUP BY d.date, c.channel
ORDER BY d.date;


-- =====================================================
-- 2) KPI by Channel
-- =====================================================
CREATE OR REPLACE VIEW mv_kpi_channel AS
WITH fs_agg AS (
  SELECT date_id, campaign_id,
         SUM(revenue) AS revenue,
         SUM(cost) AS cost,
         COUNT(DISTINCT sale_id) AS orders
  FROM marketing.fact_sales
  GROUP BY date_id, campaign_id
),
sp_agg AS (
  SELECT date_id, campaign_id,
         SUM(spend) AS spend,
         SUM(clicks) AS clicks,
         SUM(impressions) AS impressions
  FROM marketing.fact_spend
  GROUP BY date_id, campaign_id
),
merged AS (
  SELECT
    COALESCE(fs.date_id, sp.date_id) AS date_id,
    COALESCE(fs.campaign_id, sp.campaign_id) AS campaign_id,
    COALESCE(fs.revenue,0) AS revenue,
    COALESCE(fs.cost,0) AS cost,
    COALESCE(fs.orders,0) AS orders,
    COALESCE(sp.spend,0) AS spend,
    COALESCE(sp.clicks,0) AS clicks,
    COALESCE(sp.impressions,0) AS impressions
  FROM fs_agg fs
  FULL OUTER JOIN sp_agg sp
    ON fs.date_id = sp.date_id
   AND fs.campaign_id = sp.campaign_id
)
SELECT
  c.channel,

  SUM(revenue) AS revenue,
  SUM(cost) AS cost,
  ROUND(SUM(spend)::numeric, 2) AS spend,
  SUM(revenue - cost) AS gross_profit,
  
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
  SUM(orders) AS orders,

  -- AOV
  ROUND(
    (SUM(revenue)::numeric / NULLIF(SUM(orders),0))
  , 4) AS aov,

  -- CTR
  ROUND(
    (SUM(clicks)::numeric / NULLIF(SUM(impressions),0))
  , 6) AS ctr,

  -- ROAS
  ROUND(
    (SUM(revenue)::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS roas,

  -- Profit ROAS
  ROUND(
    ((SUM(revenue) - SUM(cost))::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS profit_roas,

  -- ROI
  ROUND(
    ((SUM(revenue) - SUM(spend))::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS roi,

  -- Profit ROI
  ROUND(
    ((SUM(revenue) - SUM(cost) - SUM(spend))::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS profit_roi

FROM merged m
LEFT JOIN marketing.dim_campaigns c ON m.campaign_id = c.campaign_id
GROUP BY c.channel
ORDER BY c.channel;


-- =====================================================
-- 3) KPI by Campaign
-- =====================================================
CREATE OR REPLACE VIEW mv_kpi_campaign AS
WITH fs_agg AS (
  SELECT date_id, campaign_id,
         SUM(revenue) AS revenue,
         SUM(cost) AS cost,
         COUNT(DISTINCT sale_id) AS orders
  FROM marketing.fact_sales
  GROUP BY date_id, campaign_id
),
sp_agg AS (
  SELECT date_id, campaign_id,
         SUM(spend) AS spend,
         SUM(clicks) AS clicks,
         SUM(impressions) AS impressions
  FROM marketing.fact_spend
  GROUP BY date_id, campaign_id
),
merged AS (
  SELECT
    COALESCE(fs.date_id, sp.date_id) AS date_id,
    COALESCE(fs.campaign_id, sp.campaign_id) AS campaign_id,
    COALESCE(fs.revenue,0) AS revenue,
    COALESCE(fs.cost,0) AS cost,
    COALESCE(fs.orders,0) AS orders,
    COALESCE(sp.spend,0) AS spend,
    COALESCE(sp.clicks,0) AS clicks,
    COALESCE(sp.impressions,0) AS impressions
  FROM fs_agg fs
  FULL OUTER JOIN sp_agg sp
    ON fs.date_id = sp.date_id
   AND fs.campaign_id = sp.campaign_id
)
SELECT
  c.campaign_id,
  c.campaign_name,
  c.channel,

  SUM(revenue) AS revenue,
  SUM(cost) AS cost,
  ROUND(SUM(spend)::numeric, 2) AS spend,
  SUM(revenue - cost) AS gross_profit,
  
  SUM(clicks) AS clicks,
  SUM(impressions) AS impressions,
  SUM(orders) AS orders,

  -- AOV
  ROUND(
    (SUM(revenue)::numeric / NULLIF(SUM(orders),0))
  , 4) AS aov,

  -- CTR
  ROUND(
    (SUM(clicks)::numeric / NULLIF(SUM(impressions),0))
  , 6) AS ctr,

  -- ROAS
  ROUND(
    (SUM(revenue)::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS roas,

  -- Profit ROAS
  ROUND(
    ((SUM(revenue) - SUM(cost))::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS profit_roas,

  -- ROI
  ROUND(
    ((SUM(revenue) - SUM(spend))::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS roi,

  -- Profit ROI
  ROUND(
    ((SUM(revenue) - SUM(cost) - SUM(spend))::numeric / NULLIF(SUM(spend)::numeric,0))
  , 4) AS profit_roi

FROM merged m
LEFT JOIN marketing.dim_campaigns c ON m.campaign_id = c.campaign_id
GROUP BY c.campaign_id, c.campaign_name, c.channel
ORDER BY c.campaign_id;
