import os, json, time
from datetime import datetime, timezone, timedelta
import boto3

REGION = os.getenv("AWS_REGION", "us-east-2")
AUDIT_TABLE = os.getenv("AUDIT_TABLE", "pipeline_audit_runs")
NAMESPACE = os.getenv("GOV_NAMESPACE", "OUBT/Governance")
PROJECT = os.getenv("PROJECT", "OUBT")
DOMAIN = os.getenv("DOMAIN", "NYC_Taxi")

# Optional: Redshift for orphan rates (set if you have Redshift)
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_DB   = os.getenv("REDSHIFT_DB", "dev")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PW   = os.getenv("REDSHIFT_PW")
REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", "5439"))

cloudwatch = boto3.client("cloudwatch", region_name=REGION)
ddb = boto3.resource("dynamodb", region_name=REGION).Table(AUDIT_TABLE)

def utcnow():
    return datetime.now(timezone.utc)

def scan_recent(hours=24):
    # lightweight scan for demo; in prod use GSI on started_at_utc
    resp = ddb.scan()
    items = resp.get("Items", [])
    cutoff = utcnow() - timedelta(hours=hours)

    def parse_ts(s):
        try:
            return datetime.fromisoformat(s.replace("Z","+00:00"))
        except Exception:
            return None

    recent = []
    for it in items:
        ts = parse_ts(it.get("started_at_utc",""))
        if ts and ts >= cutoff:
            recent.append(it)
    return recent

def put_metric(name, value, unit="None", extra_dims=None):
    dims = [
        {"Name":"Project","Value":PROJECT},
        {"Name":"Domain","Value":DOMAIN},
    ]
    if extra_dims:
        dims.extend(extra_dims)

    cloudwatch.put_metric_data(
        Namespace=NAMESPACE,
        MetricData=[{
            "MetricName": name,
            "Timestamp": utcnow(),
            "Value": float(value),
            "Unit": unit,
            "Dimensions": dims
        }]
    )

def compute_pipeline_success(items):
    if not items:
        return None
    total = len(items)
    succ = sum(1 for i in items if i.get("status") == "SUCCEEDED")
    fail = sum(1 for i in items if i.get("status") == "FAILED")
    return succ/total, fail/total, total

def compute_master_age(items):
    ages = []
    for it in items:
        mc = it.get("master_check") or {}
        if "age_hours" in mc:
            try: ages.append(float(mc["age_hours"]))
            except: pass
    if not ages:
        return None
    return sum(ages)/len(ages)

def redshift_orphans():
    # optional - requires psycopg2-binary installed
    import psycopg2
    if not (REDSHIFT_HOST and REDSHIFT_USER and REDSHIFT_PW):
        return None

    q = """
    SELECT
      100.0 * SUM(CASE WHEN pu_zone_sk IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS orphan_pu_pct,
      100.0 * SUM(CASE WHEN do_zone_sk IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS orphan_do_pct
    FROM analytics.fact_yellow_trip
    WHERE pickup_ts >= dateadd(day, -7, getdate());
    """
    conn = psycopg2.connect(host=REDSHIFT_HOST, dbname=REDSHIFT_DB, user=REDSHIFT_USER, password=REDSHIFT_PW, port=REDSHIFT_PORT)
    cur = conn.cursor()
    cur.execute(q)
    row = cur.fetchone()
    cur.close(); conn.close()
    return float(row[0] or 0.0), float(row[1] or 0.0)

def main():
    items = scan_recent(hours=24)

    succ = compute_pipeline_success(items)
    if succ:
        success_rate, fail_rate, total = succ
        put_metric("PipelineSuccessRate", success_rate, unit="None")
        put_metric("PipelineFailRate", fail_rate, unit="None")
        put_metric("PipelineRuns24h", total, unit="Count")

    master_avg_age = compute_master_age(items)
    if master_avg_age is not None:
        put_metric("MasterFreshnessAgeHours", master_avg_age, unit="None")

    # Optional: orphan rates from Redshift
    if REDSHIFT_HOST:
        try:
            pu, do = redshift_orphans()
            put_metric("OrphanRatePU_pct_7d", pu, unit="None")
            put_metric("OrphanRateDO_pct_7d", do, unit="None")
        except Exception as e:
            print("Redshift orphan calc skipped:", e)

    print("Published governance metrics to CloudWatch:", NAMESPACE)

if __name__ == "__main__":
    main()
