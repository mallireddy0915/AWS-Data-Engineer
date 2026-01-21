import os
import boto3
from urllib.parse import quote_plus

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
BUCKET = os.getenv("NYC_BUCKET")

ZONES = {
    "landing/":   {"Zone": "Landing",   "Governance": "Low",    "Access": "Restricted"},
    "raw/":       {"Zone": "Raw",       "Governance": "Low",    "Access": "Restricted"},
    "validated/": {"Zone": "Validated", "Governance": "Medium", "Access": "Internal"},
    "curated/":   {"Zone": "Curated",   "Governance": "High",   "Access": "Internal"},
    "master/":    {"Zone": "Master",    "Governance": "VeryHigh","Access":"Strict"},
    "archive/":   {"Zone": "Archive",   "Governance": "High",   "Access": "Strict"},
    "lineage/":   {"Zone": "Lineage",   "Governance": "High",   "Access": "Strict"},
}

COMMON_TAGS = {
    "Project": os.getenv("PROJECT_NAME", "OUBT"),
    "Domain": os.getenv("DATA_DOMAIN", "NYC_Taxi"),
    "Owner": os.getenv("DATA_OWNER", "DataEngineering"),
    "Classification": os.getenv("CLASSIFICATION", "Internal"),
}

def tag_str(tags: dict) -> str:
    return "&".join([f"{quote_plus(k)}={quote_plus(v)}" for k, v in tags.items()])

def main():
    if not BUCKET:
        raise SystemExit("Set NYC_BUCKET env var to your S3 bucket name.")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    for prefix, ztags in ZONES.items():
        key = prefix  # folder marker object
        tags = {**COMMON_TAGS, **ztags}

        # create zero-byte marker if not exists
        s3.put_object(Bucket=BUCKET, Key=key, Body=b"", Tagging=tag_str(tags))
        print(f"Created+Tagged s3://{BUCKET}/{key} tags={tags}")

if __name__ == "__main__":
    main()
