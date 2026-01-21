import os, json, time, zipfile, pathlib
import boto3
from botocore.exceptions import ClientError

REGION = os.getenv("AWS_REGION", "us-east-2")
CONFIG_PATH = os.getenv("DAY11_CONFIG", "governance/pipeline/day11_pipeline_config.json")

PROJECT = "oobt"
DDB_TABLE = "pipeline_audit_runs"
SNS_TOPIC_NAME = "mdm-steward-alerts"
STATE_MACHINE_NAME = "day11-governed-pipeline"
SCHEDULE_NAME = "nyc-taxi-pipeline-daily"

iam = boto3.client("iam", region_name=REGION)
lambda_client = boto3.client("lambda", region_name=REGION)
sfn = boto3.client("stepfunctions", region_name=REGION)
scheduler = boto3.client("scheduler", region_name=REGION)
dynamodb = boto3.client("dynamodb", region_name=REGION)
sns = boto3.client("sns", region_name=REGION)
sts = boto3.client("sts", region_name=REGION)

def load_config():
    return json.loads(pathlib.Path(CONFIG_PATH).read_text(encoding="utf-8"))

def zip_dir(src_dir: str, out_zip: str):
    src = pathlib.Path(src_dir)
    out = pathlib.Path(out_zip)
    out.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
        for p in src.rglob("*"):
            if p.is_file():
                z.write(p, p.relative_to(src))
    return str(out)

def ensure_ddb_table():
    try:
        dynamodb.describe_table(TableName=DDB_TABLE)
        print(f"ℹ️ DynamoDB exists: {DDB_TABLE}")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    dynamodb.create_table(
        TableName=DDB_TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "execution_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "execution_id", "KeyType": "HASH"}],
        Tags=[{"Key": "Project", "Value": "OUBT"}, {"Key": "Domain", "Value": "NYC_Taxi"}]
    )
    print(f"✅ Created DynamoDB table: {DDB_TABLE}")

    # wait ready
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=DDB_TABLE)

def ensure_sns_topic():
    # create_topic is idempotent
    resp = sns.create_topic(Name=SNS_TOPIC_NAME, Tags=[{"Key":"Project","Value":"OUBT"},{"Key":"Domain","Value":"NYC_Taxi"}])
    topic_arn = resp["TopicArn"]
    print(f"✅ SNS topic: {topic_arn}")
    return topic_arn

def ensure_role(role_name: str, assume_policy: dict):
    try:
        role = iam.get_role(RoleName=role_name)["Role"]
        return role["Arn"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
    resp = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=json.dumps(assume_policy),
        Description="Day11 orchestration role",
        Tags=[{"Key":"Project","Value":"OUBT"},{"Key":"Domain","Value":"NYC_Taxi"}],
    )
    return resp["Role"]["Arn"]

def put_inline_policy(role_name: str, policy_name: str, policy_doc: dict):
    iam.put_role_policy(
        RoleName=role_name,
        PolicyName=policy_name,
        PolicyDocument=json.dumps(policy_doc)
    )

def ensure_roles(bucket: str, dq_results_prefix: str):
    account_id = sts.get_caller_identity()["Account"]

    # 1) Lambda execution role
    lambda_role_name = f"{PROJECT}-day11-lambda-role"
    lambda_assume = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole"
        }]
    }
    lambda_role_arn = ensure_role(lambda_role_name, lambda_assume)

    lambda_policy = {
        "Version":"2012-10-17",
        "Statement":[
            {"Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"},
            {"Effect":"Allow","Action":["dynamodb:PutItem","dynamodb:UpdateItem","dynamodb:GetItem"],"Resource":f"arn:aws:dynamodb:{REGION}:{account_id}:table/{DDB_TABLE}"},
            {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":f"arn:aws:s3:::{bucket}"},
            {"Effect":"Allow","Action":["s3:GetObject"],"Resource":f"arn:aws:s3:::{bucket}/*"},
            {"Effect":"Allow","Action":["sns:Publish"],"Resource":"*"},
            {"Effect":"Allow","Action":["glue:StartDataQualityRulesetEvaluationRun","glue:GetDataQualityRulesetEvaluationRun"],"Resource":"*"}
        ]
    }
    put_inline_policy(lambda_role_name, "day11-lambda-inline", lambda_policy)

    # 2) Step Functions execution role (invokes lambdas + glue job sync)
    sfn_role_name = f"{PROJECT}-day11-sfn-role"
    sfn_assume = {
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow","Principal":{"Service":"states.amazonaws.com"},"Action":"sts:AssumeRole"}]
    }
    sfn_role_arn = ensure_role(sfn_role_name, sfn_assume)

    sfn_policy = {
        "Version":"2012-10-17",
        "Statement":[
            {"Effect":"Allow","Action":["lambda:InvokeFunction"],"Resource":"*"},
            {"Effect":"Allow","Action":["glue:StartJobRun","glue:GetJobRun","glue:GetJobRuns","glue:BatchStopJobRun"],"Resource":"*"},
            {"Effect":"Allow","Action":["logs:CreateLogDelivery","logs:GetLogDelivery","logs:UpdateLogDelivery","logs:DeleteLogDelivery","logs:ListLogDeliveries","logs:PutResourcePolicy","logs:DescribeResourcePolicies","logs:DescribeLogGroups"],"Resource":"*"}
        ]
    }
    put_inline_policy(sfn_role_name, "day11-sfn-inline", sfn_policy)

    # 3) Scheduler role (starts state machine)
    scheduler_role_name = f"{PROJECT}-day11-scheduler-role"
    scheduler_assume = {
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow","Principal":{"Service":"scheduler.amazonaws.com"},"Action":"sts:AssumeRole"}]
    }
    scheduler_role_arn = ensure_role(scheduler_role_name, scheduler_assume)

    scheduler_policy = {
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow","Action":["states:StartExecution"],"Resource":"*"}]
    }
    put_inline_policy(scheduler_role_name, "day11-scheduler-inline", scheduler_policy)

    # 4) Glue DQ evaluation role (assumed by Glue)
    glue_dq_role_name = f"{PROJECT}-day11-glue-dq-role"
    glue_assume = {
        "Version":"2012-10-17",
        "Statement":[{"Effect":"Allow","Principal":{"Service":"glue.amazonaws.com"},"Action":"sts:AssumeRole"}]
    }
    glue_dq_role_arn = ensure_role(glue_dq_role_name, glue_assume)

    # allow read data + write results prefix
    # results prefix is s3://bucket/path/
    _, _, rest = dq_results_prefix.partition("s3://")
    res_bucket, _, res_prefix = rest.partition("/")
    glue_dq_policy = {
        "Version":"2012-10-17",
        "Statement":[
            {"Effect":"Allow","Action":["s3:ListBucket"],"Resource":[f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{res_bucket}"]},
            {"Effect":"Allow","Action":["s3:GetObject"],"Resource":[f"arn:aws:s3:::{bucket}/*"]},
            {"Effect":"Allow","Action":["s3:PutObject"],"Resource":[f"arn:aws:s3:::{res_bucket}/{res_prefix}*"]},
            {"Effect":"Allow","Action":["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"],"Resource":"*"}
        ]
    }
    put_inline_policy(glue_dq_role_name, "day11-glue-dq-inline", glue_dq_policy)

    # IAM can take a moment to propagate
    time.sleep(8)

    return lambda_role_arn, sfn_role_arn, scheduler_role_arn, glue_dq_role_arn

def upsert_lambda(function_name: str, role_arn: str, zip_path: str, handler: str, env: dict):
    try:
        lambda_client.get_function(FunctionName=function_name)
        exists = True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            exists = False
        else:
            raise

    code_bytes = pathlib.Path(zip_path).read_bytes()
    if not exists:
        resp = lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.12",
            Role=role_arn,
            Handler=handler,
            Code={"ZipFile": code_bytes},
            Timeout=60,
            MemorySize=256,
            Environment={"Variables": env},
            Tags={"Project":"OUBT","Domain":"NYC_Taxi"}
        )
        arn = resp["FunctionArn"]
        print(f"Created Lambda: {function_name}")
        return arn

    lambda_client.update_function_code(FunctionName=function_name, ZipFile=code_bytes)
    lambda_client.update_function_configuration(FunctionName=function_name, Role=role_arn, Handler=handler, Runtime="python3.12", Timeout=60, MemorySize=256, Environment={"Variables": env})
    arn = lambda_client.get_function(FunctionName=function_name)["Configuration"]["FunctionArn"]
    print(f"Updated Lambda: {function_name}")
    return arn

def build_state_machine_def(audit_arn, fresh_arn, dq_arn, notify_arn, glue_job_name):
    # Step Functions Glue integration: glue:startJobRun.sync :contentReference[oaicite:3]{index=3}
    return {
        "Comment": "Day11 governed pipeline: audit + master freshness + Glue ETL + Glue DQ + SNS alerts",
        "StartAt": "InitAudit",
        "States": {
            "InitAudit": {
                "Type": "Task",
                "Resource": audit_arn,
                "Parameters": {
                    "mode": "init",
                    "execution_id.$": "$$.Execution.Id",
                    "trigger.$": "$.trigger",
                    "inputs.$": "$.inputs"
                },
                "Retry": [{"ErrorEquals": ["States.ALL"], "IntervalSeconds": 2, "MaxAttempts": 3, "BackoffRate": 2.0}],
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure"}],
                "Next": "MasterFreshness"
            },
            "MasterFreshness": {
                "Type": "Task",
                "Resource": fresh_arn,
                "ResultPath": "$.master_check",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure"}],
                "Next": "LogMaster"
            },
            "LogMaster": {
                "Type": "Task",
                "Resource": audit_arn,
                "Parameters": {"mode":"update","execution_id.$":"$$.Execution.Id","updates":{"master_check.$":"$.master_check"}},
                "Next": "MasterFreshChoice"
            },
            "MasterFreshChoice": {
                "Type": "Choice",
                "Choices": [{"Variable":"$.master_check.fresh","BooleanEquals": True,"Next":"RunGlueETL"}],
                "Default": "NotifyMasterStale"
            },
            "NotifyMasterStale": {
                "Type": "Task",
                "Resource": notify_arn,
                "Parameters": {"subject":"Master data stale: steward review needed","execution_id.$":"$$.Execution.Id","master_check.$":"$.master_check","action":"MASTER_STALE_ALERT"},
                "Next": "RunGlueETL"
            },
            "RunGlueETL": {
                "Type": "Task",
                "Resource": "arn:aws:states:::glue:startJobRun.sync",
                "Parameters": {
                    "JobName": glue_job_name,
                    "Arguments.$": "$.glue_arguments"
                },
                "ResultPath": "$.glue_run",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyFailure"}],
                "Next": "LogGlue"
            },
            "LogGlue": {
                "Type": "Task",
                "Resource": audit_arn,
                "Parameters": {"mode":"update","execution_id.$":"$$.Execution.Id","updates":{"glue_run.$":"$.glue_run"}},
                "Next": "RunDQGate"
            },
            "RunDQGate": {
                "Type": "Task",
                "Resource": dq_arn,
                "ResultPath": "$.dq",
                "Catch": [{"ErrorEquals": ["States.ALL"], "Next": "NotifyDQFailure"}],
                "Next": "LogDQ"
            },
            "NotifyDQFailure": {
                "Type": "Task",
                "Resource": notify_arn,
                "Parameters": {"subject":"Data Quality gate FAILED: pipeline blocked","execution_id.$":"$$.Execution.Id","action":"DQ_FAILED","details.$":"$"},
                "Next": "FinalizeFail"
            },
            "LogDQ": {
                "Type": "Task",
                "Resource": audit_arn,
                "Parameters": {"mode":"update","execution_id.$":"$$.Execution.Id","updates":{"dq.$":"$.dq"}},
                "Next": "FinalizeSuccess"
            },
            "FinalizeSuccess": {
                "Type": "Task",
                "Resource": audit_arn,
                "Parameters": {"mode":"update","execution_id.$":"$$.Execution.Id","updates":{"status":"SUCCEEDED"}},
                "Next": "Done"
            },
            "FinalizeFail": {
                "Type": "Task",
                "Resource": audit_arn,
                "Parameters": {"mode":"update","execution_id.$":"$$.Execution.Id","updates":{"status":"FAILED"}},
                "Next": "FailState"
            },
            "NotifyFailure": {
                "Type": "Task",
                "Resource": notify_arn,
                "Parameters": {"subject":"Pipeline execution FAILED (system error)","execution_id.$":"$$.Execution.Id","action":"PIPELINE_FAILED","details.$":"$"},
                "Next": "FinalizeFail"
            },
            "FailState": {"Type": "Fail", "Error":"PipelineFailed", "Cause":"See audit table + SNS notifications"},
            "Done": {"Type": "Succeed"}
        }
    }

def upsert_state_machine(definition: dict, role_arn: str):
    sm_arn = None
    # find existing by name
    resp = sfn.list_state_machines()
    for sm in resp.get("stateMachines", []):
        if sm["name"] == STATE_MACHINE_NAME:
            sm_arn = sm["stateMachineArn"]
            break

    if sm_arn:
        sfn.update_state_machine(stateMachineArn=sm_arn, definition=json.dumps(definition), roleArn=role_arn)
        print(f"Updated state machine: {sm_arn}")
        return sm_arn

    create = sfn.create_state_machine(
        name=STATE_MACHINE_NAME,
        definition=json.dumps(definition),
        roleArn=role_arn,
        type="STANDARD",
        tags=[{"key":"Project","value":"OUBT"},{"key":"Domain","value":"NYC_Taxi"}]
    )
    sm_arn = create["stateMachineArn"]
    print(f"Created state machine: {sm_arn}")
    return sm_arn

def upsert_schedule(state_machine_arn: str, scheduler_role_arn: str, schedule_expr: str, payload: dict):
    try:
        scheduler.get_schedule(Name=SCHEDULE_NAME)
        exists = True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            exists = False
        else:
            raise

    params = {
        "Name": SCHEDULE_NAME,
        "ScheduleExpression": schedule_expr,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "Target": {
            "Arn": state_machine_arn,
            "RoleArn": scheduler_role_arn,
            "Input": json.dumps(payload)
        }
    }

    if not exists:
        scheduler.create_schedule(**params)
        print(f"Created schedule: {SCHEDULE_NAME}")
        return

    scheduler.update_schedule(**params)
    print(f"Updated schedule: {SCHEDULE_NAME}")

def main():
    cfg = load_config()
    bucket = cfg["bucket"]

    ensure_ddb_table()
    topic_arn = ensure_sns_topic()

    lambda_role_arn, sfn_role_arn, scheduler_role_arn, glue_dq_role_arn = ensure_roles(bucket, cfg["dq_results_s3_prefix"])

    # Package lambdas
    z_audit = zip_dir("lambda/day11_audit_logger", "tmp/day11_audit_logger.zip")
    z_fresh = zip_dir("lambda/day11_master_freshness", "tmp/day11_master_freshness.zip")
    z_dq    = zip_dir("lambda/day11_dq_gate", "tmp/day11_dq_gate.zip")
    z_note  = zip_dir("lambda/day11_notify_steward", "tmp/day11_notify_steward.zip")

    audit_arn = upsert_lambda(
        f"{PROJECT}-day11-audit-logger", lambda_role_arn, z_audit, "app.handler",
        {"DDB_TABLE": DDB_TABLE}
    )
    fresh_arn = upsert_lambda(
        f"{PROJECT}-day11-master-freshness", lambda_role_arn, z_fresh, "app.handler",
        {"BUCKET": bucket, "MASTER_PREFIX": cfg["master_snapshot_prefix"], "MAX_AGE_HOURS": str(cfg["master_max_age_hours"])}
    )
    dq_arn = upsert_lambda(
        f"{PROJECT}-day11-dq-gate", lambda_role_arn, z_dq, "app.handler",
        {
            "GLUE_DB": cfg["glue_catalog_db"],
            "GLUE_TABLE": cfg["glue_catalog_table"],
            "RULESET_NAME": cfg["dq_ruleset_name"],
            "GLUE_DQ_ROLE_ARN": glue_dq_role_arn,
            "RESULTS_S3_PREFIX": cfg["dq_results_s3_prefix"]
        }
    )
    notify_arn = upsert_lambda(
        f"{PROJECT}-day11-notify-steward", lambda_role_arn, z_note, "app.handler",
        {"TOPIC_ARN": topic_arn}
    )

    # State machine definition
    definition = build_state_machine_def(audit_arn, fresh_arn, dq_arn, notify_arn, cfg["glue_job_name"])
    sm_arn = upsert_state_machine(definition, sfn_role_arn)

    # Scheduler payload (who triggered + what data processed)
    payload = {
        "trigger": {"type":"schedule","name": SCHEDULE_NAME},
        "inputs": {
            "bucket": bucket,
            "zones_master_prefix": cfg["master_snapshot_prefix"],
            "dq_ruleset": cfg["dq_ruleset_name"]
        },
        "glue_arguments": {
            "--SOURCE_TRIPS_S3": f"s3://{bucket}/validated/yellow/2025/08/",
            "--SOURCE_ZONES_S3": f"s3://{bucket}/raw/reference/taxi_zone_lookup.csv",
            "--TARGET_CURATED_S3": f"s3://{bucket}/curated/yellow/",
            "--LINEAGE_S3": f"s3://{bucket}/lineage/glue/day11_run.json"
        }
    }

    upsert_schedule(sm_arn, scheduler_role_arn, cfg["schedule_expression"], payload)

    print("\nDay11 Orchestration deployed.")
    print("State machine ARN:", sm_arn)
    print("SNS topic ARN:", topic_arn)
    print("Audit table:", DDB_TABLE)
    print("\nNext: subscribe your email/phone to the SNS topic in the AWS console to receive alerts.")

if __name__ == "__main__":
    main()
