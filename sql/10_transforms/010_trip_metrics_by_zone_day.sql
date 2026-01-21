CREATE TABLE IF NOT EXISTS analytics.trip_metrics_by_zone_day (
  trip_date DATE NOT NULL,
  pu_location_id INT NOT NULL,
  pu_zone TEXT,
  pu_borough TEXT,
  trip_count BIGINT NOT NULL,
  total_revenue NUMERIC NOT NULL,
  avg_trip_distance NUMERIC,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_by TEXT NOT NULL DEFAULT 'sql_transform_v1',
  PRIMARY KEY (trip_date, pu_location_id)
);

WITH base AS (
  SELECT
    (tpep_pickup_datetime::date) AS trip_date,
    pulocationid AS pu_location_id,
    pu_zone,
    pu_borough,
    trip_distance,
    total_amount
  FROM analytics.curated_yellow_trips
),
agg AS (
  SELECT
    trip_date,
    pu_location_id,
    MAX(pu_zone) AS pu_zone,
    MAX(pu_borough) AS pu_borough,
    COUNT(*) AS trip_count,
    SUM(COALESCE(total_amount,0)) AS total_revenue,
    AVG(COALESCE(trip_distance,0)) AS avg_trip_distance
  FROM base
  GROUP BY 1,2
)
INSERT INTO analytics.trip_metrics_by_zone_day (
  trip_date, pu_location_id, pu_zone, pu_borough, trip_count, total_revenue, avg_trip_distance
)
SELECT * FROM agg
ON CONFLICT (trip_date, pu_location_id)
DO UPDATE SET
  pu_zone = EXCLUDED.pu_zone,
  pu_borough = EXCLUDED.pu_borough,
  trip_count = EXCLUDED.trip_count,
  total_revenue = EXCLUDED.total_revenue,
  avg_trip_distance = EXCLUDED.avg_trip_distance;