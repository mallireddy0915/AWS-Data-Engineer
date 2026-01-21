import os
import json
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("NYC_BUCKET")

TRIPS_PARQUET = os.getenv("TRIPS_PARQUET", "yellow_tripdata_2025-08.parquet")
OUT_LOCAL = os.getenv("VALIDATED_OUT", "tmp/validated_yellow_2025-08.parquet")

S3_KEY = os.getenv("VALIDATED_S3_KEY", "validated/yellow/2025/08/yellow_tripdata_2025-08_validated.parquet")
LINEAGE_KEY = os.getenv("LINEAGE_S3_KEY", "lineage/yellow/2025/08/promote_validated.json")

def main():
    if not BUCKET:
        raise SystemExit("Set NYC_BUCKET env var.")

    Path("tmp").mkdir(exist_ok=True)

    df = pd.read_parquet(TRIPS_PARQUET)

    # Basic quality gates (Validated zone)
    mask = pd.Series(True, index=df.index)

    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        mask &= df["tpep_pickup_datetime"] <= df["tpep_dropoff_datetime"]

    if "trip_distance" in df.columns:
        mask &= df["trip_distance"].fillna(0) >= 0

    if "total_amount" in df.columns:
        mask &= df["total_amount"].fillna(0) >= 0

    before = len(df)
    dfv = df.loc[mask].copy()
    after = len(dfv)

    dfv.to_parquet(OUT_LOCAL, index=False)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(OUT_LOCAL, BUCKET, S3_KEY)

    lineage = {
        "run_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "stage": "validated_promotion",
        "input": {"local_parquet": TRIPS_PARQUET},
        "output": {"s3": f"s3://{BUCKET}/{S3_KEY}"},
        "quality_gate": {
            "rules": ["pickup<=dropoff", "trip_distance>=0", "total_amount>=0"],
            "rows_in": before,
            "rows_out": after,
            "rows_dropped": before - after
        },
        "timestamp_utc": datetime.utcnow().isoformat() + "Z"
    }

    lineage_local = "tmp/lineage_validated.json"
    Path(lineage_local).write_text(json.dumps(lineage, indent=2), encoding="utf-8")
    s3.upload_file(lineage_local, BUCKET, LINEAGE_KEY)

    print(f"Validated dataset uploaded: s3://{BUCKET}/{S3_KEY}")
    print(f"Lineage uploaded: s3://{BUCKET}/{LINEAGE_KEY}")
    print(f"Rows in: {before} Rows out: {after} Dropped: {before-after}")

if __name__ == "__main__":
    main()
