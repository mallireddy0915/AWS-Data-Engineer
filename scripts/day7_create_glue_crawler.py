import os
import boto3
from botocore.exceptions import ClientError

AWS_REGION = os.getenv("AWS_REGION", "us-east-2")
GLUE_DB = os.getenv("GLUE_DB", "nyc_taxi_db")
CRAWLER_NAME = os.getenv("CRAWLER_NAME", "nyc_taxi_curated_crawler")

S3_TARGET = os.getenv("CRAWLER_S3_TARGET")  # e.g. s3://bucket/curated/yellow/
# IAM role for Glue (service principal glue.amazonaws.com)
GLUE_ROLE_ARN = os.getenv("GLUE_ROLE_ARN")


def ensure_db(glue):
    try:
        glue.create_database(DatabaseInput={"Name": GLUE_DB})
        print(f" Created Glue DB: {GLUE_DB}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f" Glue DB exists: {GLUE_DB}")
        else:
            raise


def main():
    if not S3_TARGET:
        raise SystemExit(
            "Set CRAWLER_S3_TARGET (e.g., s3://bucket/curated/yellow/)")
    if not GLUE_ROLE_ARN:
        raise SystemExit("Set GLUE_ROLE_ARN (Glue service role ARN)")

    glue = boto3.client("glue", region_name=AWS_REGION)

    ensure_db(glue)

    # Create crawler
    try:
        glue.create_crawler(
            Name=CRAWLER_NAME,
            Role=GLUE_ROLE_ARN,
            DatabaseName=GLUE_DB,
            Targets={"S3Targets": [{"Path": S3_TARGET}]},
            TablePrefix="curated_",
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "DEPRECATE_IN_DATABASE"
            }
        )
        print(f" Created crawler: {CRAWLER_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f" Crawler exists: {CRAWLER_NAME}")
        else:
            raise

    glue.start_crawler(Name=CRAWLER_NAME)
    print(f" Started crawler: {CRAWLER_NAME}")


if __name__ == "__main__":
    main()