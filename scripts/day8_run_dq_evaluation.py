import os
import boto3

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
RULESET_NAME = os.getenv("DQ_RULESET_NAME")
GLUE_DB = os.getenv("GLUE_DB", "nyc_taxi_db")
TABLE_NAME = os.getenv("DQ_TABLE")
RESULTS_S3_PREFIX = os.getenv("DQ_RESULTS_S3_PREFIX")  # e.g. s3://bucket/lineage/dq-results/

def main():
    if not all([RULESET_NAME, TABLE_NAME, RESULTS_S3_PREFIX]):
        raise SystemExit("Set DQ_RULESET_NAME, DQ_TABLE, DQ_RESULTS_S3_PREFIX env vars.")

    glue = boto3.client("glue", region_name=AWS_REGION)
    resp = glue.start_data_quality_ruleset_evaluation_run(
        DataSource={"GlueTable": {"DatabaseName": GLUE_DB, "TableName": TABLE_NAME}},
        Role=os.getenv("GLUE_ROLE_ARN"),
        RulesetNames=[RULESET_NAME],
        AdditionalRunOptions={
            "CloudWatchMetricsEnabled": True,
            "ResultsS3Prefix": RESULTS_S3_PREFIX
        }
    )
    print("Started evaluation run:", resp["RunId"])

if __name__ == "__main__":
    main()
