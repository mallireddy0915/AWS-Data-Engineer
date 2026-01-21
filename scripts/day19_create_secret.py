import os, json, boto3

REGION = os.getenv("AWS_REGION","us-east-2")
SECRET_NAME = os.getenv("SECRET_NAME","oobt/rds/postgres_admin")
RDS_HOST = os.getenv("RDS_HOST")  # set this
RDS_DB   = os.getenv("RDS_DB","postgres")
USERNAME = os.getenv("RDS_USER","postgres")
PASSWORD = os.getenv("RDS_PASSWORD")  # set this

def main():
    if not (RDS_HOST and PASSWORD):
        raise SystemExit("Set RDS_HOST and RDS_PASSWORD env vars")

    sm = boto3.client("secretsmanager", region_name=REGION)
    payload = {
        "engine":"postgres",
        "host":RDS_HOST,
        "username":USERNAME,
        "password":PASSWORD,
        "dbname":RDS_DB,
        "port":5432
    }

    try:
        sm.create_secret(Name=SECRET_NAME, SecretString=json.dumps(payload))
        print("Created secret:", SECRET_NAME)
    except sm.exceptions.ResourceExistsException:
        sm.put_secret_value(SecretId=SECRET_NAME, SecretString=json.dumps(payload))
        print("Updated secret:", SECRET_NAME)

if __name__ == "__main__":
    main()
