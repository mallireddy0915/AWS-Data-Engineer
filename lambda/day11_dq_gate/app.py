import os, time
import boto3

GLUE_DB = os.environ["GLUE_DB"]
GLUE_TABLE = os.environ["GLUE_TABLE"]
RULESET_NAME = os.environ["RULESET_NAME"]
GLUE_DQ_ROLE_ARN = os.environ["GLUE_DQ_ROLE_ARN"]
RESULTS_S3_PREFIX = os.environ["RESULTS_S3_PREFIX"]

POLL_SECONDS = int(os.environ.get("POLL_SECONDS", "15"))
TIMEOUT_SECONDS = int(os.environ.get("TIMEOUT_SECONDS", "900"))

glue = boto3.client("glue")

def handler(event, context):
    start = glue.start_data_quality_ruleset_evaluation_run(
        DataSource={"GlueTable": {"DatabaseName": GLUE_DB, "TableName": GLUE_TABLE}},
        Role=GLUE_DQ_ROLE_ARN,
        RulesetNames=[RULESET_NAME],
        AdditionalRunOptions={
            "CloudWatchMetricsEnabled": True,
            "ResultsS3Prefix": RESULTS_S3_PREFIX
        }
    )
    run_id = start["RunId"]

    deadline = time.time() + TIMEOUT_SECONDS
    status = "STARTING"
    last = None

    while time.time() < deadline:
        last = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
        status = last.get("Status", status)
        if status in ("SUCCEEDED", "FAILED", "STOPPED", "TIMEOUT"):
            break
        time.sleep(POLL_SECONDS)

    if status != "SUCCEEDED":
        raise Exception(f"DQ evaluation did not succeed. RunId={run_id} Status={status}")

    return {"dq_run_id": run_id, "status": status}
