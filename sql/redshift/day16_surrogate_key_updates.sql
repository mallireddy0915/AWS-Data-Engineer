-- Date dim seed (for Aug 2025 example)
INSERT INTO analytics.dim_date(full_date, year, month, day, day_of_week)
SELECT d::date, EXTRACT(year FROM d), EXTRACT(month FROM d), EXTRACT(day FROM d), EXTRACT(dow FROM d)
FROM (
  SELECT DISTINCT pickup_ts::date AS d
  FROM analytics.fact_yellow_trip
  WHERE pickup_ts IS NOT NULL
) s;

-- Add pickup_date + date_sk
UPDATE analytics.fact_yellow_trip f
SET pickup_date = f.pickup_ts::date;

UPDATE analytics.fact_yellow_trip f
SET date_sk = d.date_sk
FROM analytics.dim_date d
WHERE f.pickup_date = d.full_date;

-- Vendor SK
UPDATE analytics.fact_yellow_trip f
SET vendor_sk = v.vendor_sk
FROM analytics.dim_vendor v
WHERE f.vendor_id = v.vendor_id;

-- Zone SKs
UPDATE analytics.fact_yellow_trip f
SET pu_zone_sk = z.zone_sk
FROM analytics.dim_zone z
WHERE f.pu_location_id = z.location_id;

UPDATE analytics.fact_yellow_trip f
SET do_zone_sk = z.zone_sk
FROM analytics.dim_zone z
WHERE f.do_location_id = z.location_id;
