import boto3, os

REGION = os.getenv("AWS_REGION", "us-east-2")
BUCKET = "arjun-s3-776312084600"
WORKGROUP = os.getenv("ATHENA_WG", "oobt_analytics")
RESULTS = f"s3://{BUCKET}/athena-results/{WORKGROUP}/"
BYTES_CUTOFF = int(os.getenv("BYTES_CUTOFF", str(50 * 1024**3)))  # 50GB

def main():
    athena = boto3.client("athena", region_name=REGION)

    try:
        athena.get_work_group(WorkGroup=WORKGROUP)
        exists = True
    except athena.exceptions.InvalidRequestException:
        exists = False

    config = {
        "EnforceWorkGroupConfiguration": True,
        "PublishCloudWatchMetricsEnabled": True,
        "ResultConfiguration": {"OutputLocation": RESULTS},
        "BytesScannedCutoffPerQuery": BYTES_CUTOFF,
    }

    if not exists:
        athena.create_work_group(
            Name=WORKGROUP,
            Configuration=config,
            Description="OUBT Athena workgroup with governance guardrails"
        )
        print(f"Created workgroup {WORKGROUP} output={RESULTS} cutoff={BYTES_CUTOFF}")
    else:
        athena.update_work_group(
            WorkGroup=WORKGROUP,
            ConfigurationUpdates=config,
            Description="OUBT Athena workgroup with governance guardrails"
        )
        print(f"Updated workgroup {WORKGROUP} output={RESULTS} cutoff={BYTES_CUTOFF}")

if __name__ == "__main__":
    main()
