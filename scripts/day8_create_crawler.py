import os
import boto3
from botocore.exceptions import ClientError

AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_DB = os.getenv("GLUE_DB", "nyc_taxi_db")
CRAWLER_NAME = os.getenv("CRAWLER_NAME", "nyc_taxi_day8_crawler")
S3_TARGET = os.getenv("CRAWLER_S3_TARGET")        # e.g. s3://bucket/validated/yellow/
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")        # Glue service role arn

def main():
    if not S3_TARGET or not GLUE_ROLE_ARN:
        raise SystemExit("Set CRAWLER_S3_TARGET and GLUE_ROLE_ARN env vars.")

    glue = boto3.client("glue", region_name=AWS_REGION)

    # Ensure DB
    try:
        glue.create_database(DatabaseInput={"Name": GLUE_DB})
        print(f"Created DB {GLUE_DB}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise

    # Create crawler
    try:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=GLUE_ROLE_ARN,
            DatabaseName=GLUE_DB,
            Targets={"S3Targets": [{"Path": S3_TARGET}]},
            TablePrefix="day8_",
            SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "DEPRECATE_IN_DATABASE"},
        )
        print(f"Created crawler {CRAWLER_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        print(f"Crawler already exists: {CRAWLER_NAME}")

    glue.start_crawler(Name=CRAWLER_NAME)
    print(f"Started crawler: {CRAWLER_NAME}")

if __name__ == "__main__":
    main()
