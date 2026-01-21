WITH failures AS (
  SELECT 1
  FROM analytics.trip_metrics_by_zone_day
  WHERE trip_date IS NULL OR pu_location_id IS NULL
)
SELECT COUNT(*) AS failing_rows FROM failures;