import os, boto3

REGION = os.getenv("AWS_REGION","us-east-2")
LOG_GROUP = os.getenv("CLOUDTRAIL_LOG_GROUP","/aws/cloudtrail/oobt")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

FILTER_NAME = "OUBT-S3-DeleteObject"
METRIC_NS = "OUBT/Security"
METRIC_NAME = "S3DeleteObjectCount"

def main():
    if not SNS_TOPIC_ARN:
        raise SystemExit("Set SNS_TOPIC_ARN")

    logs = boto3.client("logs", region_name=REGION)
    cw = boto3.client("cloudwatch", region_name=REGION)

    # Filter pattern: looks for DeleteObject events
    # (CloudWatch Logs filter pattern syntax is documented.) :contentReference[oaicite:9]{index=9}
    pattern = '{ ($.eventSource = "s3.amazonaws.com") && ($.eventName = "DeleteObject") }'

    logs.put_metric_filter(
        logGroupName=LOG_GROUP,
        filterName=FILTER_NAME,
        filterPattern=pattern,
        metricTransformations=[{
            "metricNamespace": METRIC_NS,
            "metricName": METRIC_NAME,
            "metricValue": "1"
        }]
    )

    cw.put_metric_alarm(
        AlarmName="OUBT-Security-S3DeleteObject-Alarm",
        Namespace=METRIC_NS,
        MetricName=METRIC_NAME,
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching"
    )

    print("CloudTrail metric filter + alarm configured")

if __name__ == "__main__":
    main()
