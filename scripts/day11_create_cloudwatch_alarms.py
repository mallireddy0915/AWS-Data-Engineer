import os
import boto3

REGION = os.getenv("AWS_REGION", "us-east-2")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")  # from deploy output
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")

def main():
    if not SNS_TOPIC_ARN or not STATE_MACHINE_ARN:
        raise SystemExit("Set SNS_TOPIC_ARN and STATE_MACHINE_ARN")

    cw = boto3.client("cloudwatch", region_name=REGION)

    # Step Functions ExecutionsFailed alarm
    cw.put_metric_alarm(
        AlarmName="Day11-StepFunctions-ExecutionsFailed",
        Namespace="AWS/States",
        MetricName="ExecutionsFailed",
        Dimensions=[{"Name":"StateMachineArn","Value":STATE_MACHINE_ARN}],
        Statistic="Sum",
        Period=300,
        EvaluationPeriods=1,
        Threshold=1,
        ComparisonOperator="GreaterThanOrEqualToThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="notBreaching"
    )
    print("Created alarm: Day11-StepFunctions-ExecutionsFailed")

if __name__ == "__main__":
    main()
