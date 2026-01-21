import os, json
from pathlib import Path
from datetime import datetime
import boto3

OUT_MD = Path("docs/quality_scorecard.md")
OUT_JSON = Path("docs/quality_scorecard.json")

BUCKET = os.getenv("NYC_BUCKET")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
S3_KEY = os.getenv("SCORECARD_S3_KEY", "lineage/quality/day8_quality_scorecard.json")

def main():
    Path("docs").mkdir(exist_ok=True)

    # Map quality dimension -> business impact -> owner -> threshold -> action
    scorecard = [
        {
            "dimension": "Completeness",
            "business_metric": "Can analytics trust pickup/dropoff timestamps?",
            "owner": "Data Steward (Ops)",
            "threshold": ">= 99.9% non-null pickup/dropoff",
            "action_on_failure": "Block promotion to validated; create ticket"
        },
        {
            "dimension": "Validity",
            "business_metric": "Are distances/charges valid for billing/finance dashboards?",
            "owner": "Data Owner (Finance)",
            "threshold": "100% trip_distance >= 0 and total_amount >= 0",
            "action_on_failure": "Quarantine bad rows; alert owner"
        },
        {
            "dimension": "Consistency",
            "business_metric": "Do trips match known zones (no orphan zones)?",
            "owner": "Data Steward (Ops)",
            "threshold": "<= 0.1% orphan PU/DO",
            "action_on_failure": "Manual review; steward approval required"
        },
        {
            "dimension": "Timeliness",
            "business_metric": "Are monthly datasets available for reporting on time?",
            "owner": "Data Custodian (DE)",
            "threshold": "Curated dataset published within 24h of ingest",
            "action_on_failure": "On-call alert; rerun pipeline"
        }
    ]

    OUT_JSON.write_text(json.dumps({
        "generated_utc": datetime.utcnow().isoformat() + "Z",
        "domain": os.getenv("DATA_DOMAIN", "NYC_Taxi"),
        "classification": os.getenv("CLASSIFICATION", "Internal"),
        "scorecard": scorecard
    }, indent=2), encoding="utf-8")

    # Markdown
    md = ["# Day 8 — Data Quality Scorecard (Governance Context)\n\n"]
    md.append("| Quality Dimension | Business Metric | Owner | Threshold | Action on Failure |\n")
    md.append("|---|---|---|---|---|\n")
    for r in scorecard:
        md.append(f"| {r['dimension']} | {r['business_metric']} | {r['owner']} | {r['threshold']} | {r['action_on_failure']} |\n")
    OUT_MD.write_text("".join(md), encoding="utf-8")

    print(f"Wrote {OUT_MD} and {OUT_JSON}")

    # Optional upload
    if BUCKET:
        s3 = boto3.client("s3", region_name=AWS_REGION)
        s3.upload_file(str(OUT_JSON), BUCKET, S3_KEY)
        print(f"Uploaded scorecard to s3://{BUCKET}/{S3_KEY}")

if __name__ == "__main__":
    main()
