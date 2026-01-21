import sys, json
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lower, trim

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "SOURCE_CSV_S3",
    "TARGET_PARQUET_S3",
    "QUALITY_REPORT_S3",
    "MAX_BAD_PCT"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

src = args["SOURCE_CSV_S3"]
tgt = args["TARGET_PARQUET_S3"]
rep = args["QUALITY_REPORT_S3"]
max_bad_pct = float(args["MAX_BAD_PCT"])

df = spark.read.option("header", True).csv(src)
total = df.count()

# Quality rules (example for zones):
# - LocationID not null and unique
# - Borough/Zone/service_zone not null
# - service_zone in allowed set (normalize)
allowed_service = {"boro zone", "yellow zone", "ewr"}

df2 = df.withColumn("LocationID", col("LocationID").cast("int")) \
        .withColumn("service_zone_n", lower(trim(col("service_zone"))))

bad_locationid_null = df2.filter(col("LocationID").isNull()).count()
bad_borough_null = df2.filter(col("Borough").isNull()).count()
bad_zone_null = df2.filter(col("Zone").isNull()).count()
bad_service_null = df2.filter(col("service_zone").isNull()).count()
bad_service_invalid = df2.filter(~col("service_zone_n").isin(list(allowed_service))).count()

distinct_ids = df2.select("LocationID").distinct().count()
bad_dupes = total - distinct_ids

bad_total = bad_locationid_null + bad_borough_null + bad_zone_null + bad_service_null + bad_service_invalid + bad_dupes
bad_pct = (bad_total / total * 100.0) if total else 0.0

quality_report = {
    "job": args["JOB_NAME"],
    "timestamp_utc": datetime.utcnow().isoformat() + "Z",
    "input": src,
    "output": tgt,
    "total_rows": total,
    "checks": {
        "LocationID_not_null_bad": bad_locationid_null,
        "Borough_not_null_bad": bad_borough_null,
        "Zone_not_null_bad": bad_zone_null,
        "service_zone_not_null_bad": bad_service_null,
        "service_zone_allowed_bad": bad_service_invalid,
        "LocationID_duplicates_bad": bad_dupes
    },
    "bad_total": bad_total,
    "bad_pct": bad_pct,
    "threshold_max_bad_pct": max_bad_pct,
    "status": "PASS" if bad_pct <= max_bad_pct else "FAIL"
}

# Write validated parquet (even if fail; you can change to write only on pass)
df2.drop("service_zone_n").write.mode("overwrite").parquet(tgt)

# Write quality report to S3
import boto3
def parse_s3(uri):
    _, _, rest = uri.partition("s3://")
    b, _, k = rest.partition("/")
    return b, k

b, k = parse_s3(rep)
boto3.client("s3").put_object(Bucket=b, Key=k, Body=json.dumps(quality_report, indent=2).encode("utf-8"))

# Gate (fail job if bad > threshold)
if bad_pct > max_bad_pct:
    raise Exception(f"Data quality gate failed: bad_pct={bad_pct:.4f} > {max_bad_pct}")

job.commit()
