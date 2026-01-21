import os
import json
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("NYC_BUCKET")

ZONES_CSV = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")
VALIDATED_LOCAL = os.getenv("VALIDATED_LOCAL", "tmp/validated_yellow_2025-08.parquet")

OUT_LOCAL = os.getenv("CURATED_OUT", "tmp/curated_yellow_2025-08_enriched.parquet")
S3_KEY = os.getenv("CURATED_S3_KEY", "curated/yellow/2025/08/yellow_tripdata_2025-08_curated.parquet")
LINEAGE_KEY = os.getenv("CURATED_LINEAGE_KEY", "lineage/yellow/2025/08/build_curated.json")

def main():
    if not BUCKET:
        raise SystemExit("Set NYC_BUCKET env var.")

    Path("tmp").mkdir(exist_ok=True)

    zones = pd.read_csv(ZONES_CSV)
    zones = zones.rename(columns={"LocationID":"LocationID","Borough":"Borough","Zone":"Zone","service_zone":"service_zone"})
    zones = zones[["LocationID","Borough","Zone","service_zone"]].copy()

    df = pd.read_parquet(VALIDATED_LOCAL)

    # Enrich PU
    df = df.merge(
        zones.add_prefix("PU_"),
        how="left",
        left_on="PULocationID",
        right_on="PU_LocationID"
    )

    # Enrich DO
    df = df.merge(
        zones.add_prefix("DO_"),
        how="left",
        left_on="DOLocationID",
        right_on="DO_LocationID"
    )

    # Simple curated business rule example: add trip_duration_minutes
    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        dur = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds() / 60.0
        df["trip_duration_minutes"] = dur

    df.to_parquet(OUT_LOCAL, index=False)

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(OUT_LOCAL, BUCKET, S3_KEY)

    lineage = {
        "run_id": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "stage": "curated_enrichment",
        "inputs": [
            {"local_parquet": VALIDATED_LOCAL},
            {"local_csv": ZONES_CSV}
        ],
        "output": {"s3": f"s3://{BUCKET}/{S3_KEY}"},
        "transformations": [
            "join PULocationID to zones",
            "join DOLocationID to zones",
            "derive trip_duration_minutes"
        ],
        "timestamp_utc": datetime.utcnow().isoformat() + "Z"
    }

    lineage_local = "tmp/lineage_curated.json"
    Path(lineage_local).write_text(json.dumps(lineage, indent=2), encoding="utf-8")
    s3.upload_file(lineage_local, BUCKET, LINEAGE_KEY)

    print(f"Curated dataset uploaded: s3://{BUCKET}/{S3_KEY}")
    print(f"Lineage uploaded: s3://{BUCKET}/{LINEAGE_KEY}")

if __name__ == "__main__":
    main()
