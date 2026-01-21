import sys
import json
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, year, month, lit, when

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_TRIPS_S3",
    "SOURCE_ZONES_S3",
    "TARGET_CURATED_S3",
    "LINEAGE_S3"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

source_trips = args["SOURCE_TRIPS_S3"]     # e.g. s3://bucket/validated/yellow/2025/08/...
source_zones = args["SOURCE_ZONES_S3"]     # e.g. s3://bucket/bronze/reference/taxi_zone_lookup.csv
target_curated = args["TARGET_CURATED_S3"] # e.g. s3://bucket/curated/yellow/
lineage_s3 = args["LINEAGE_S3"]            # e.g. s3://bucket/lineage/glue/day7_run.json

# Read parquet trips
trips = spark.read.parquet(source_trips)

# Read zones csv
zones = spark.read.option("header", True).csv(source_zones)
zones = zones.withColumn("LocationID", col("LocationID").cast("int"))

trips = trips.withColumn("PULocationID", col("PULocationID").cast("int")) \
             .withColumn("DOLocationID", col("DOLocationID").cast("int")) \
             .withColumn("tpep_pickup_datetime", col("tpep_pickup_datetime").cast("timestamp")) \
             .withColumn("tpep_dropoff_datetime", col("tpep_dropoff_datetime").cast("timestamp"))

# Quality gates (Validated)
before = trips.count()

trips_q = trips.filter(col("tpep_pickup_datetime").isNotNull()) \
               .filter(col("tpep_dropoff_datetime").isNotNull()) \
               .filter(col("tpep_pickup_datetime") <= col("tpep_dropoff_datetime")) \
               .filter(when(col("trip_distance").isNull(), lit(0)).otherwise(col("trip_distance")) >= 0) \
               .filter(when(col("total_amount").isNull(), lit(0)).otherwise(col("total_amount")) >= 0)

after = trips_q.count()

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

# Select only needed columns (optimize output size)
keep_cols = [
    "VendorID","tpep_pickup_datetime","tpep_dropoff_datetime","passenger_count",
    "trip_distance","RatecodeID","PULocationID","DOLocationID","payment_type",
    "fare_amount","tip_amount","total_amount",
    "PU_Borough","PU_Zone","PU_service_zone",
    "DO_Borough","DO_Zone","DO_service_zone",
    "year","month"
]
trips_out = trips_q.select(*[c for c in keep_cols if c in trips_q.columns])

# Write curated parquet partitioned
trips_out.write.mode("overwrite").partitionBy("year","month").parquet(target_curated)

# Write lineage JSON (simple governance artifact)
lineage = {
    "job": args["JOB_NAME"],
    "timestamp_utc": datetime.utcnow().isoformat() + "Z",
    "inputs": {
        "trips": source_trips,
        "zones": source_zones
    },
    "output": {
        "curated": target_curated
    },
    "quality_gate": {
        "rules": ["pickup<=dropoff","trip_distance>=0","total_amount>=0"],
        "rows_in": before,
        "rows_out": after,
        "rows_dropped": before - after
    },
    "transformations": [
        "filter validated rules",
        "add partitions year/month",
        "join PU and DO zone attributes"
    ]
}

# Save lineage to S3
import boto3
s3 = boto3.client("s3")
def parse_s3(uri):
    assert uri.startswith("s3://")
    _, _, rest = uri.partition("s3://")
    b, _, k = rest.partition("/")
    return b, k

b, k = parse_s3(lineage_s3)
s3.put_object(Bucket=b, Key=k, Body=json.dumps(lineage, indent=2).encode("utf-8"))

job.commit()
