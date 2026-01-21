import os, json
import boto3
from pathlib import Path

REGION = os.getenv("AWS_REGION","us-east-2")
DASHBOARD_NAME = os.getenv("DASHBOARD_NAME","OUBT-Observability")
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")
PATH = os.getenv("DASHBOARD_JSON","dashboards/day18/oobt_observability_dashboard.json")

def main():
    if not STATE_MACHINE_ARN:
        raise SystemExit("Set STATE_MACHINE_ARN")

    body = Path(PATH).read_text(encoding="utf-8")
    body = body.replace("__STATE_MACHINE_ARN__", STATE_MACHINE_ARN)

    cw = boto3.client("cloudwatch", region_name=REGION)
    cw.put_dashboard(DashboardName=DASHBOARD_NAME, DashboardBody=body)
    print(f"Dashboard upserted: {DASHBOARD_NAME}")

if __name__ == "__main__":
    main()
