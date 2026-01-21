CREATE SCHEMA IF NOT EXISTS certified;

-- Example safe view (adapt fields to your fact table)
CREATE OR REPLACE VIEW certified.vw_fact_yellow_trip_safe AS
SELECT
  trip_id,
  pickup_ts,
  dropoff_ts,
  pu_zone_sk,
  do_zone_sk,
  vendor_sk,
  total_amount,
  fare_amount,
  trip_distance,
  passenger_count
FROM analytics.fact_yellow_trip;

-- Principle: grant only certified schema to analysts
-- (role name example; adjust to your environment)
GRANT USAGE ON SCHEMA certified TO ROLE analyst_role;
GRANT SELECT ON ALL TABLES IN SCHEMA certified TO ROLE analyst_role;

REVOKE ALL ON SCHEMA analytics FROM ROLE analyst_role;
REVOKE ALL ON ALL TABLES IN SCHEMA analytics FROM ROLE analyst_role;
