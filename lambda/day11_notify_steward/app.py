import os, json
import boto3

TOPIC_ARN = os.environ["TOPIC_ARN"]
sns = boto3.client("sns")

def handler(event, context):
    subject = event.get("subject", "NYC Taxi Pipeline Alert")[:100]
    message = json.dumps(event, indent=2)
    sns.publish(TopicArn=TOPIC_ARN, Subject=subject, Message=message)
    return {"notified": True}
