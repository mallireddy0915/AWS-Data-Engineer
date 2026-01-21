WITH failures AS (
  SELECT 1
  FROM analytics.trip_metrics_by_zone_day
  WHERE total_revenue < 0
)
SELECT COUNT(*) AS failing_rows FROM failures;