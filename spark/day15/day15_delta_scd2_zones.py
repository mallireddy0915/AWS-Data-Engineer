import os
from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws, current_timestamp
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

ZONES_CSV = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")
DELTA_PATH = os.getenv("DELTA_PATH", "tmp/delta_zone_scd2")
RUN_ID = os.getenv("RUN_ID", datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))

def spark_session():
    builder = (
        SparkSession.builder
        .appName("Day15 Delta SCD2 Zones")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    spark = spark_session()

    incoming = spark.read.option("header", True).csv(ZONES_CSV) \
        .withColumn("LocationID", col("LocationID").cast("int")) \
        .withColumnRenamed("LocationID", "location_id") \
        .withColumnRenamed("Borough", "borough") \
        .withColumnRenamed("Zone", "zone") \
        .withColumnRenamed("service_zone", "service_zone")

    # hash to detect changes (NULL-safe)
    incoming = incoming.withColumn(
        "attr_hash",
        sha2(concat_ws("||", col("borough"), col("zone"), col("service_zone")), 256)
    ).withColumn("change_batch_id", lit(RUN_ID))

    if not DeltaTable.isDeltaTable(spark, DELTA_PATH):
        # initial load as SCD2 v1
        init = incoming.select(
            "location_id","borough","zone","service_zone","attr_hash","change_batch_id"
        ).withColumn("version_number", lit(1)) \
         .withColumn("effective_from", current_timestamp()) \
         .withColumn("effective_to", lit(None).cast("timestamp")) \
         .withColumn("is_current", lit(True))

        init.write.format("delta").mode("overwrite").save(DELTA_PATH)
        print(f"Created Delta SCD2 table at {DELTA_PATH}")
        spark.stop()
        return

    dt = DeltaTable.forPath(spark, DELTA_PATH)
    current_df = dt.toDF().filter(col("is_current") == True)

    # identify changed records
    joined = incoming.alias("i").join(
        current_df.select("location_id","attr_hash","version_number").alias("c"),
        on="location_id",
        how="left"
    )

    changed = joined.filter(col("c.location_id").isNull() | (col("i.attr_hash") != col("c.attr_hash")))
    # new_version = current_version+1 else 1
    changed = changed.withColumn(
        "new_version",
        (col("c.version_number") + lit(1)).cast("int")
    ).na.fill({"new_version": 1})

    # 1) expire current rows for changed keys
    dt.alias("t").merge(
        changed.select(col("location_id").alias("k")).distinct().alias("s"),
        "t.location_id = s.k AND t.is_current = true"
    ).whenMatchedUpdate(set={
        "is_current": "false",
        "effective_to": "current_timestamp()"
    }).execute()

    # 2) insert new current versions for changed keys
    new_rows = changed.select(
        col("i.location_id").alias("location_id"),
        col("i.borough").alias("borough"),
        col("i.zone").alias("zone"),
        col("i.service_zone").alias("service_zone"),
        col("i.attr_hash").alias("attr_hash"),
        col("i.change_batch_id").alias("change_batch_id"),
        col("new_version").alias("version_number")
    ).withColumn("effective_from", current_timestamp()) \
     .withColumn("effective_to", lit(None).cast("timestamp")) \
     .withColumn("is_current", lit(True))

    new_rows.write.format("delta").mode("append").save(DELTA_PATH)
    print("Applied SCD2 changes in Delta.")

    # Show time travel versions
    hist = spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_PATH}`")
    hist.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
