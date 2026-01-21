-- Run in Athena in workgroup oobt_analytics

CREATE TABLE curated_yellow_part
WITH (
  format = 'PARQUET',
  parquet_compression = 'SNAPPY',
  external_location = 's3://arjun-s3-776312084600/athena-analytics/curated_yellow/',
  partitioned_by = ARRAY['year','month']
) AS
SELECT
  t.*,
  year(tpep_pickup_datetime) AS year,
  month(tpep_pickup_datetime) AS month
FROM curated_yellow t;
