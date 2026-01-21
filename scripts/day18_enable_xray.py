import os, boto3

REGION = os.getenv("AWS_REGION","us-east-2")
LAMBDA_NAMES = os.getenv("LAMBDA_NAMES","oobt-day11-audit-logger,oobt-day11-master-freshness,oobt-day11-dq-gate,oobt-day11-notify-steward").split(",")

def main():
    lam = boto3.client("lambda", region_name=REGION)
    for fn in [x.strip() for x in LAMBDA_NAMES if x.strip()]:
        lam.update_function_configuration(
            FunctionName=fn,
            TracingConfig={"Mode":"Active"}
        )
        print("Enabled X-Ray tracing for", fn)

if __name__ == "__main__":
    main()
