-- Day 3 SQL Schema (Postgres)

CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.taxi_zone_lookup;
CREATE TABLE staging.taxi_zone_lookup (
  LocationID INT PRIMARY KEY,
  Borough TEXT,
  Zone TEXT,
  service_zone TEXT
);

DROP TABLE IF EXISTS staging.yellow_trips;
CREATE TABLE staging.yellow_trips (
  VendorID INT,
  tpep_pickup_datetime TIMESTAMP,
  tpep_dropoff_datetime TIMESTAMP,
  passenger_count INT,
  trip_distance NUMERIC,
  RatecodeID INT,
  store_and_fwd_flag TEXT,
  PULocationID INT,
  DOLocationID INT,
  payment_type INT,
  fare_amount NUMERIC,
  extra NUMERIC,
  mta_tax NUMERIC,
  tip_amount NUMERIC,
  tolls_amount NUMERIC,
  improvement_surcharge NUMERIC,
  total_amount NUMERIC,
  congestion_surcharge NUMERIC,
  airport_fee NUMERIC,
  cbd_congestion_fee NUMERIC
);