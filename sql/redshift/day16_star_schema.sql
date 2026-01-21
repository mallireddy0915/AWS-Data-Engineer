-- Schema
CREATE SCHEMA IF NOT EXISTS analytics;

-- Date dimension
CREATE TABLE IF NOT EXISTS analytics.dim_date (
  date_sk INT IDENTITY(1,1),
  full_date DATE NOT NULL,
  year SMALLINT,
  month SMALLINT,
  day SMALLINT,
  day_of_week SMALLINT,
  PRIMARY KEY(date_sk)
)
DISTSTYLE ALL
SORTKEY(full_date);

-- Zone dimension (derived from MDM golden records)
CREATE TABLE IF NOT EXISTS analytics.dim_zone (
  zone_sk INT IDENTITY(1,1),
  location_id INT NOT NULL,
  borough VARCHAR(50),
  zone VARCHAR(100),
  service_zone VARCHAR(50),
  mdm_version_number INT,
  mdm_effective_from TIMESTAMP,
  mdm_is_current BOOLEAN,
  mdm_approved_by VARCHAR(100),
  mdm_approved_at TIMESTAMP,
  PRIMARY KEY(zone_sk)
)
DISTSTYLE ALL
SORTKEY(location_id);

-- Vendor dimension
CREATE TABLE IF NOT EXISTS analytics.dim_vendor (
  vendor_sk INT IDENTITY(1,1),
  vendor_id INT NOT NULL,
  vendor_name VARCHAR(200),
  lifecycle_state VARCHAR(20),
  PRIMARY KEY(vendor_sk)
)
DISTSTYLE ALL
SORTKEY(vendor_id);

-- Fact table (big)
CREATE TABLE IF NOT EXISTS analytics.fact_yellow_trip (
  trip_id BIGINT IDENTITY(1,1),

  pickup_ts TIMESTAMP,
  dropoff_ts TIMESTAMP,
  pickup_date DATE,
  date_sk INT,

  vendor_id INT,
  vendor_sk INT,

  pu_location_id INT,
  pu_zone_sk INT,
  do_location_id INT,
  do_zone_sk INT,

  passenger_count INT,
  trip_distance DOUBLE PRECISION,

  fare_amount DOUBLE PRECISION,
  total_amount DOUBLE PRECISION,
  payment_type INT,
  ratecodeid INT,

  PRIMARY KEY(trip_id)
)
-- For a first cut: AUTO lets Redshift optimize distribution. :contentReference[oaicite:4]{index=4}
DISTSTYLE AUTO
-- For time-based analytics, timestamp leading sort key helps range scans. :contentReference[oaicite:5]{index=5}
SORTKEY(pickup_ts);
