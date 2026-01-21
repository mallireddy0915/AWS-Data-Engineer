import os
import boto3

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
SCHEDULE_NAME = os.getenv("SCHEDULE_NAME", "nyc-taxi-pipeline-daily")
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")
SCHEDULER_ROLE_ARN = os.getenv("SCHEDULER_ROLE_ARN")  # role that can StartExecution
CRON = os.getenv("CRON", "cron(0 6 * * ? *)")  # 6:00 UTC daily

def main():
    if not STATE_MACHINE_ARN or not SCHEDULER_ROLE_ARN:
        raise SystemExit("Set STATE_MACHINE_ARN and SCHEDULER_ROLE_ARN")

    scheduler = boto3.client("scheduler", region_name=AWS_REGION)

    payload = {
        "trigger": {"type": "schedule", "name": SCHEDULE_NAME},
        "config": {
            "glue_job_name": os.getenv("GLUE_JOB_NAME", "nyc-taxi-curated-job")
        },
        "glue_arguments": {
            "--SOURCE_TRIPS_S3": os.getenv("SOURCE_TRIPS_S3", ""),
            "--SOURCE_ZONES_S3": os.getenv("SOURCE_ZONES_S3", ""),
            "--TARGET_CURATED_S3": os.getenv("TARGET_CURATED_S3", ""),
            "--LINEAGE_S3": os.getenv("LINEAGE_S3", "")
        }
    }

    scheduler.create_schedule(
        Name=SCHEDULE_NAME,
        ScheduleExpression=CRON,
        FlexibleTimeWindow={"Mode": "OFF"},
        Target={
            "Arn": STATE_MACHINE_ARN,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": __import__("json").dumps(payload)
        }
    )
    print(f"Created schedule: {SCHEDULE_NAME} -> {STATE_MACHINE_ARN}")

if __name__ == "__main__":
    main()
