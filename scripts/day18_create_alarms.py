import os, boto3

REGION = os.getenv("AWS_REGION","us-east-2")
SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")       # mdm-steward-alerts ARN
STATE_MACHINE_ARN = os.getenv("STATE_MACHINE_ARN")

def main():
    if not (SNS_TOPIC_ARN and STATE_MACHINE_ARN):
        raise SystemExit("Set SNS_TOPIC_ARN and STATE_MACHINE_ARN")

    cw = boto3.client("cloudwatch", region_name=REGION)

    # Step Functions failure alarm
    cw.put_metric_alarm(
        AlarmName="OUBT-StepFunctions-ExecutionsFailed",
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

    # Governance success rate drop (custom metric)
    cw.put_metric_alarm(
        AlarmName="OUBT-Governance-PipelineSuccessRate-Low",
        Namespace="OUBT/Governance",
        MetricName="PipelineSuccessRate",
        Dimensions=[{"Name":"Project","Value":"OUBT"},{"Name":"Domain","Value":"NYC_Taxi"}],
        Statistic="Average",
        Period=300,
        EvaluationPeriods=3,
        Threshold=0.95,
        ComparisonOperator="LessThanThreshold",
        AlarmActions=[SNS_TOPIC_ARN],
        TreatMissingData="missing"
    )

    print("Alarms created/updated")

if __name__ == "__main__":
    main()
