-- Example: if you ever had a PII-like column (email/phone), mask it.
-- (NYC taxi data usually doesn’t include direct PII, but you implement the pattern anyway.)

CREATE SCHEMA IF NOT EXISTS certified;

-- Suppose fact table had rider_email (demo only)
-- Create a safe view:
CREATE OR REPLACE VIEW certified.vw_fact_yellow_trip_safe AS
SELECT
  trip_id,
  pickup_ts,
  dropoff_ts,
  pu_zone_sk,
  do_zone_sk,
  total_amount,
  -- mask email if present:
  CASE
    WHEN rider_email IS NULL THEN NULL
    ELSE regexp_replace(rider_email, '(^.).*(@.*$)', '\\1***\\2')
  END AS rider_email_masked
FROM analytics.fact_yellow_trip;

-- Grant analysts only the safe view
GRANT SELECT ON certified.vw_fact_yellow_trip_safe TO ROLE analyst_role;
REVOKE ALL ON analytics.fact_yellow_trip FROM ROLE analyst_role;
