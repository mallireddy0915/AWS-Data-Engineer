import os, json
import boto3

REGION = os.getenv("AWS_REGION", "us-east-2")
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")
BUCKET = os.getenv("BUCKET", "arjun-s3-776312084600")

def main():
    if not STATE_MACHINE_ARN:
        raise SystemExit("Set STATE_MACHINE_ARN from deploy output.")

    sfn = boto3.client("stepfunctions", region_name=REGION)

    payload = {
        "trigger": {"type":"manual","user":"arjun"},
        "inputs": {"bucket": BUCKET},
        "config": {
            "glue_job_name": "nyc-taxi-curated-job-fixed"
        },
        "glue_arguments": {
            "--SOURCE_TRIPS_S3": f"s3://{BUCKET}/validated/yellow/2025/08/",
            "--SOURCE_ZONES_S3": f"s3://{BUCKET}/bronze/reference/taxi_zone_lookup.csv",
            "--TARGET_CURATED_S3": f"s3://{BUCKET}/curated/yellow/",
            "--LINEAGE_S3": f"s3://{BUCKET}/lineage/glue/day11_manual.json"
        }
    }

    resp = sfn.start_execution(stateMachineArn=STATE_MACHINE_ARN, input=json.dumps(payload))
    print("Started execution:", resp["executionArn"])

if __name__ == "__main__":
    main()
