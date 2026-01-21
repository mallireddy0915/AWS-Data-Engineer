-- Certified: only curated, no raw columns, enforce partition filters through convention

CREATE OR REPLACE VIEW nyc_taxi_athena.vw_certified_yellow_trips AS
SELECT
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  total_amount
FROM nyc_taxi_athena.curated_yellow_pp;
