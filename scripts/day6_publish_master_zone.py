import os
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
BUCKET = os.getenv("NYC_BUCKET")
ZONES_CSV = os.getenv("ZONES_CSV", "taxi_zone_lookup.csv")

def main():
    if not BUCKET:
        raise SystemExit("Set NYC_BUCKET env var.")

    Path("tmp").mkdir(exist_ok=True)

    zones = pd.read_csv(ZONES_CSV)
    # treat as golden snapshot (in reality you’d pull from mdm.dim_zone in RDS)
    out = f"tmp/master_zones_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    zones.to_parquet(out, index=False)

    s3_key = f"master/zones/snapshots/{Path(out).name}"
    boto3.client("s3", region_name=AWS_REGION).upload_file(out, BUCKET, s3_key)

    print(f"Published master zone snapshot: s3://{BUCKET}/{s3_key}")

if __name__ == "__main__":
    main()
