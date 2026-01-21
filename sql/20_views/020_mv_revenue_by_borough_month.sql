CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.mv_revenue_by_borough_month AS
WITH m AS (
  SELECT
    date_trunc('month', trip_date)::date AS month,
    pu_borough,
    SUM(total_revenue) AS revenue
  FROM analytics.trip_metrics_by_zone_day
  GROUP BY 1,2
)
SELECT * FROM m;

CREATE INDEX IF NOT EXISTS idx_mv_rev_borough_month
ON analytics.mv_revenue_by_borough_month(month, pu_borough);