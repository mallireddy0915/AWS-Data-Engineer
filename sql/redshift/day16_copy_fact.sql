-- Make sure the bucket region matches the cluster region. :contentReference[oaicite:10]{index=10}

COPY analytics.fact_yellow_trip(
  pickup_ts,
  dropoff_ts,
  vendor_id,
  pu_location_id,
  do_location_id,
  passenger_count,
  trip_distance,
  fare_amount,
  total_amount,
  payment_type,
  ratecodeid
)
FROM 's3://arjun-s3-776312084600/curated/yellow/2025/08/'
IAM_ROLE 'arn:aws:iam::776312084600:role/oobt-redshift-copy-role'
FORMAT AS PARQUET;
