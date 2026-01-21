import os, datetime
import boto3

BUCKET = os.environ["BUCKET"]
MASTER_PREFIX = os.environ["MASTER_PREFIX"]
MAX_AGE_HOURS = int(os.environ.get("MAX_AGE_HOURS", "168"))

s3 = boto3.client("s3")

def handler(event, context):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=MASTER_PREFIX)
    objs = resp.get("Contents", [])
    if not objs:
        return {"fresh": False, "reason": "no_master_snapshots_found"}

    latest = max(objs, key=lambda o: o["LastModified"])
    last_modified = latest["LastModified"]
    age_hours = (datetime.datetime.now(datetime.timezone.utc) - last_modified).total_seconds() / 3600.0

    return {
        "fresh": age_hours <= MAX_AGE_HOURS,
        "age_hours": round(age_hours, 2),
        "latest_key": latest["Key"],
        "last_modified": last_modified.isoformat()
    }
