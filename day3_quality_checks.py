import os
import json
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime

# ---- CONFIG ----
PARQUET_PATH = os.getenv("TRIPS_PARQUET", "yellow_tripdata_2025-08.parquet")
ZONES_CSV_PATH = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")

# If you set these env vars, report uploads to S3
BUCKET = os.getenv("NYC_BUCKET")  
S3_REPORT_PREFIX = os.getenv("S3_REPORT_PREFIX", "metadata/quality_reports/")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

OUT_DIR = Path("docs")
OUT_DIR.mkdir(exist_ok=True)

def upload_to_s3(local_path: Path, bucket: str, key: str):
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.upload_file(str(local_path), bucket, key)
    return f"s3://{bucket}/{key}"

def main():
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report_path = OUT_DIR / f"quality_report_{ts}.json"

    zones = pd.read_csv(ZONES_CSV_PATH)
    trips = pd.read_parquet(PARQUET_PATH)

    report = {
        "generated_utc": datetime.utcnow().isoformat() + "Z",
        "inputs": {
            "parquet": PARQUET_PATH,
            "zones_csv": ZONES_CSV_PATH
        },
        "row_count": int(len(trips)),
        "checks": {},
        "samples": {},
    }

    def add_check(name, bad_mask, sample_cols=None):
        bad_count = int(bad_mask.sum())
        report["checks"][name] = {
            "bad_count": bad_count,
            "bad_pct": round((bad_count / len(trips) * 100) if len(trips) else 0, 6),
            "status": "PASS" if bad_count == 0 else "FAIL"
        }
        if bad_count > 0:
            sample_cols = sample_cols or []
            sample = trips.loc[bad_mask, sample_cols].head(10).copy()
            # Convert Timestamps to strings for JSON serialization
            for col in sample.columns:
                if pd.api.types.is_datetime64_any_dtype(sample[col]):
                    sample[col] = sample[col].astype(str)
            report["samples"][name] = sample.to_dict(orient="records")

    # Check 1: pickup <= dropoff
    if "tpep_pickup_datetime" in trips.columns and "tpep_dropoff_datetime" in trips.columns:
        bad = trips["tpep_pickup_datetime"] > trips["tpep_dropoff_datetime"]
        add_check(
            "pickup_after_dropoff",
            bad.fillna(False),
            sample_cols=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "total_amount"]
        )

    # Check 2: trip_distance >= 0
    if "trip_distance" in trips.columns:
        bad = trips["trip_distance"].fillna(0) < 0
        add_check("negative_trip_distance", bad, sample_cols=["trip_distance", "PULocationID", "DOLocationID"])

    # Check 3: total_amount >= 0
    if "total_amount" in trips.columns:
        bad = trips["total_amount"].fillna(0) < 0
        add_check("negative_total_amount", bad, sample_cols=["total_amount", "fare_amount", "tip_amount", "tolls_amount"])

    # Check 4/5: location IDs exist in zone lookup
    if "LocationID" in zones.columns:
        zone_ids = set(zones["LocationID"].dropna().astype(int).tolist())

        if "PULocationID" in trips.columns:
            pu = trips["PULocationID"].dropna().astype(int)
            bad = ~pu.isin(zone_ids)
            # align back to original index
            bad_mask = trips["PULocationID"].notna() & ~trips["PULocationID"].astype(int).isin(zone_ids)
            add_check("pu_location_missing_in_lookup", bad_mask, sample_cols=["PULocationID"])

        if "DOLocationID" in trips.columns:
            bad_mask = trips["DOLocationID"].notna() & ~trips["DOLocationID"].astype(int).isin(zone_ids)
            add_check("do_location_missing_in_lookup", bad_mask, sample_cols=["DOLocationID"])

    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote report: {report_path}")

    # Optional upload
    if BUCKET:
        key = f"{S3_REPORT_PREFIX}{report_path.name}"
        s3_uri = upload_to_s3(report_path, BUCKET, key)
        print(f"Uploaded report to: {s3_uri}")
    else:
        print("NYC_BUCKET not set; skipping S3 upload. (export NYC_BUCKET=your-bucket to enable)")

if __name__ == "__main__":
    main()