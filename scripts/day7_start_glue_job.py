import os
import boto3

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
JOB_NAME = os.getenv("GLUE_JOB_NAME")  # existing Glue job name

def main():
    if not JOB_NAME:
        raise SystemExit("Set GLUE_JOB_NAME")

    glue = boto3.client("glue", region_name=AWS_REGION)

    args = {
        "--SOURCE_TRIPS_S3": os.getenv("SOURCE_TRIPS_S3"),
        "--SOURCE_ZONES_S3": os.getenv("SOURCE_ZONES_S3"),
        "--TARGET_CURATED_S3": os.getenv("TARGET_CURATED_S3"),
        "--LINEAGE_S3": os.getenv("LINEAGE_S3"),
    }
    # remove None
    args = {k: v for k, v in args.items() if v}

    resp = glue.start_job_run(JobName=JOB_NAME, Arguments=args)
    print("Started Glue job run:", resp["JobRunId"])

if __name__ == "__main__":
    main()
