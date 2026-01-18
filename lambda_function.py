import boto3
import os

REGION = "ap-south-1"
LAUNCH_TEMPLATE_NAME = "trading-bot"

ec2 = boto3.client("ec2", region_name=REGION)

def lambda_handler(event, context):
    response = ec2.run_instances(
        LaunchTemplate={
            "LaunchTemplateName": LAUNCH_TEMPLATE_NAME,
            "Version": "$Latest"
        },
        MinCount=1,
        MaxCount=1
    )

    instance_id = response["Instances"][0]["InstanceId"]

    return {
        "status": "EC2 launched",
        "instance_id": instance_id
    }
