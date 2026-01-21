-- 1) Total trips
SELECT COUNT(*) AS trip_count
FROM staging.yellow_trips;

-- 2) Trips by payment type
SELECT payment_type, COUNT(*) AS trips
FROM staging.yellow_trips
GROUP BY payment_type
ORDER BY trips DESC;

-- 3) Top pickup zones by trip count
SELECT z.Borough, z.Zone, COUNT(*) AS trips
FROM staging.yellow_trips t
JOIN staging.taxi_zone_lookup z
  ON t.PULocationID = z.LocationID
GROUP BY z.Borough, z.Zone
ORDER BY trips DESC
LIMIT 20;

-- 4) CTE: Top zones by revenue
WITH zone_rev AS (
  SELECT PULocationID, SUM(COALESCE(total_amount,0)) AS revenue
  FROM staging.yellow_trips
  GROUP BY PULocationID
)
SELECT z.Borough, z.Zone, zr.revenue
FROM zone_rev zr
JOIN staging.taxi_zone_lookup z
  ON zr.PULocationID = z.LocationID
ORDER BY zr.revenue DESC
LIMIT 10;

-- 5) Window: rank zones by revenue within borough
WITH zone_rev AS (
  SELECT PULocationID, SUM(COALESCE(total_amount,0)) AS revenue
  FROM staging.yellow_trips
  GROUP BY PULocationID
)
SELECT
  z.Borough,
  z.Zone,
  zr.revenue,
  DENSE_RANK() OVER (PARTITION BY z.Borough ORDER BY zr.revenue DESC) AS borough_rank
FROM zone_rev zr
JOIN staging.taxi_zone_lookup z
  ON zr.PULocationID = z.LocationID
ORDER BY z.Borough, borough_rank, zr.revenue DESC;