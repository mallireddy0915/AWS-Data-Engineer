import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

DELTA_PATH = os.getenv("DELTA_PATH", "tmp/delta_zone_scd2")
TARGET_VERSION = int(os.getenv("TARGET_VERSION", "0"))

def spark_session():
    builder = (
        SparkSession.builder
        .appName("Day15 Delta Rollback Restore")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()

def main():
    spark = spark_session()

    # Read an older version
    old = spark.read.format("delta").option("versionAsOf", TARGET_VERSION).load(DELTA_PATH)
    print(f"Loaded version {TARGET_VERSION} rowcount={old.count()}")

    # Restore by overwriting current table with that snapshot (simple rollback strategy)
    old.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(DELTA_PATH)
    print(f"Rolled back Delta table by overwriting with version {TARGET_VERSION}")

    spark.stop()

if __name__ == "__main__":
    main()
