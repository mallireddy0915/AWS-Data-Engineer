import os
from pathlib import Path
import boto3

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
GLUE_DB = os.getenv("GLUE_DB", "nyc_taxi_db")
TABLE_NAME = os.getenv("DQ_TABLE")                 # e.g. day8_validated_yellow
RULESET_NAME = os.getenv("DQ_RULESET_NAME", "yellow_trips_ruleset_v1")
DQDL_PATH = os.getenv("DQDL_PATH", "governance/dq/yellow_trips_ruleset.dqdl")
DESCRIPTION = os.getenv("DQ_DESC", "Day8 core validated rules")

def main():
    if not TABLE_NAME:
        raise SystemExit("Set DQ_TABLE env var to the Glue Catalog table name.")

    dqdl = Path(DQDL_PATH).read_text(encoding="utf-8")
    glue = boto3.client("glue", region_name=AWS_REGION)

    resp = glue.create_data_quality_ruleset(
        Name=RULESET_NAME,
        Description=DESCRIPTION,
        Ruleset=dqdl,
        TargetTable={"DatabaseName": GLUE_DB, "TableName": TABLE_NAME},
    )
    print("Created ruleset:", resp.get("Name", RULESET_NAME))

if __name__ == "__main__":
    main()
