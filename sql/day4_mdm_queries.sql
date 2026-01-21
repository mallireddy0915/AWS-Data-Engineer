-- 1) Check zone master data
SELECT COUNT(*) AS zones, COUNT(DISTINCT location_id) AS distinct_location_ids
FROM mdm.dim_zone;

-- 2) Latest versions (audit)
SELECT location_id, borough, zone, service_zone, version, updated_at, updated_by, approved_by
FROM mdm.dim_zone
ORDER BY updated_at DESC
LIMIT 20;

-- 3) List code sets
SELECT * FROM mdm.dim_vendor ORDER BY vendor_id;
SELECT * FROM mdm.dim_rate_code ORDER BY ratecode_id;
SELECT * FROM mdm.dim_payment_type ORDER BY payment_type_id;

-- 4) Example join (once you have staging.yellow_trips in DB)
-- Shows how consumers depend on mastered domains
SELECT
  z.borough,
  z.zone,
  COUNT(*) AS trips,
  SUM(COALESCE(t.total_amount,0)) AS revenue
FROM staging.yellow_trips t
JOIN mdm.dim_zone z
  ON t.PULocationID = z.location_id
GROUP BY z.borough, z.zone
ORDER BY revenue DESC
LIMIT 20;
