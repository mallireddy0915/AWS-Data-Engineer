CREATE DATABASE IF NOT EXISTS nyc_taxi_athena;

CREATE EXTERNAL TABLE IF NOT EXISTS nyc_taxi_athena.curated_yellow_pp (
  VendorID int,
  tpep_pickup_datetime timestamp,
  tpep_dropoff_datetime timestamp,
  passenger_count int,
  trip_distance double,
  RatecodeID int,
  PULocationID int,
  DOLocationID int,
  payment_type int,
  fare_amount double,
  total_amount double
)
PARTITIONED BY (
  year int,
  month int
)
STORED AS PARQUET
LOCATION 's3://arjun-s3-776312084600/athena-analytics/curated_yellow/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.year.type'='integer',
  'projection.year.range'='2020,2030',
  'projection.month.type'='integer',
  'projection.month.range'='1,12',
  'storage.location.template'='s3://arjun-s3-776312084600/athena-analytics/curated_yellow/year=${year}/month=${month}/'
);
