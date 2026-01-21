import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, to_timestamp, when, lit
)

TRIPS_PARQUET = os.getenv("TRIPS_PARQUET", "yellow_tripdata_2025-08.parquet")
ZONES_CSV = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")
OUT_PATH = os.getenv("OUT_PATH", "tmp/curated_spark_local")  # folder output

def main():
    spark = (
        SparkSession.builder
        .appName("NYC Taxi Day7 Local Spark")
        .getOrCreate()
    )

    trips = spark.read.parquet(TRIPS_PARQUET)
    zones = spark.read.option("header", True).csv(ZONES_CSV)

    # Cast zone key
    zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

    # Basic schema alignment
    trips = trips.withColumn("PULocationID", col("PULocationID").cast("int")) \
                 .withColumn("DOLocationID", col("DOLocationID").cast("int"))

    # Parse timestamps (Glue often already has timestamp types; local parquet likely does too)
    # This is safe in either case.
    trips = trips.withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
                 .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))

    # Quality gates (Validated rules)
    trips_q = trips.filter(col("tpep_pickup_datetime").isNotNull()) \
                   .filter(col("tpep_dropoff_datetime").isNotNull()) \
                   .filter(col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")) \
                   .filter(when(col("trip_distance").isNull(), lit(0)).otherwise(col("trip_distance")) >= 0) \
                   .filter(when(col("total_amount").isNull(), lit(0)).otherwise(col("total_amount")) >= 0)

    # Partition columns
    trips_q = trips_q.withColumn("year", year(col("tpep_pickup_datetime"))) \
                     .withColumn("month", month(col("tpep_pickup_datetime")))

    # Enrich PU
    pu = zones.select(
        col("LocationID").alias("PU_LocationID"),
        col("Borough").alias("PU_Borough"),
        col("Zone").alias("PU_Zone"),
        col("service_zone").alias("PU_service_zone"),
    )
    trips_q = trips_q.join(pu, trips_q.PULocationID == pu.PU_LocationID, "left")

    # Enrich DO
    do = zones.select(
        col("LocationID").alias("DO_LocationID"),
        col("Borough").alias("DO_Borough"),
        col("Zone").alias("DO_Zone"),
        col("service_zone").alias("DO_service_zone"),
    )
    trips_q = trips_q.join(do, trips_q.DOLocationID == do.DO_LocationID, "left")

    # Optimization basics: select only needed cols (edit list as needed)
    keep_cols = [
        "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count",
        "trip_distance","RatecodeID","PULocationID","DOLocationID","payment_type",
        "fare_amount","tip_amount","total_amount",
        "PU_Borough","PU_Zone","PU_service_zone",
        "DO_Borough","DO_Zone","DO_service_zone",
        "year","month"
    ]
    trips_out = trips_q.select(*[c for c in keep_cols if c in trips_q.columns])

    # Write partitioned parquet
    trips_out.write.mode("overwrite").partitionBy("year","month").parquet(OUT_PATH)

    print(f"Wrote curated dataset locally to: {OUT_PATH}")
    spark.stop()

if __name__ == "__main__":
    main()
