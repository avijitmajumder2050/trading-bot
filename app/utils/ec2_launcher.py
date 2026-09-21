#app/utils/c2_launcher.py
import boto3
import logging
from app.config.aws_s3 import S3_BUCKET
logger = logging.getLogger(__name__)
# ── Config ──

CSV_KEY = "uploads/nifty_15m_breakout_signals.csv"
EC2_REGION = "ap-south-1"

# SSM Parameter where Launch Template ID is stored
SSM_PARAM_NAME = "/trading-bot-algo/ec2/launch_template_id"

# Tag used to identify instances launched by this bot
INSTANCE_TAG_KEY = "Name"
INSTANCE_TAG_VALUE = "trading-bot-algo"


def get_existing_instance_id(ec2=None):
    """
    Returns the InstanceId of an already pending/running trading-bot-algo
    EC2 instance, or None if none exists.
    """
    ec2 = ec2 or boto3.client("ec2", region_name=EC2_REGION)
    try:
        response = ec2.describe_instances(
            Filters=[
                {"Name": f"tag:{INSTANCE_TAG_KEY}", "Values": [INSTANCE_TAG_VALUE]},
                {"Name": "instance-state-name", "Values": ["pending", "running"]},
            ]
        )
        for reservation in response.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                return instance["InstanceId"]
        return None
    except Exception as e:
        logger.error(f"❌ Failed to check for existing EC2 instance: {e}")
        return None


def get_launch_template_id_from_ssm():
    """Fetch Launch Template ID from SSM Parameter Store"""
    ssm = boto3.client("ssm", region_name=EC2_REGION)
    try:
        response = ssm.get_parameter(Name=SSM_PARAM_NAME)
        lt_id = response['Parameter']['Value']
        logger.info(f"📦 Launch Template ID fetched from SSM: {lt_id}")
        return lt_id
    except Exception as e:
        logger.error(f"❌ Failed to fetch Launch Template ID from SSM: {e}")
        return None


def check_csv_and_launch_ec2():
    """
    Reads CSV from S3, checks number of rows, launches EC2 if >= 3 rows
    and no trading-bot-algo instance is already running.
    """
    s3 = boto3.client("s3")
    ec2 = boto3.client("ec2", region_name=EC2_REGION)

    existing_instance_id = get_existing_instance_id(ec2)
    if existing_instance_id:
        logger.info(
            f"♻️ EC2 instance already running: {existing_instance_id} — skipping launch"
        )
        return {"status": "already_running", "instance_id": existing_instance_id}

    launch_template_id = get_launch_template_id_from_ssm()
    if not launch_template_id:
        return {"status": "failed", "error": "Launch Template ID not found"}

    try:
        # Read CSV from S3
        obj = s3.get_object(Bucket=S3_BUCKET, Key=CSV_KEY)
        lines = obj['Body'].read().decode('utf-8').splitlines()

        row_count = max(0, len(lines) - 1)  # exclude header
        logger.info(f"📊 CSV rows (excluding header): {row_count}")

        if row_count < 3:
            logger.info("⚠️ Not enough rows to launch EC2")
            return {"status": "not_enough_rows"}

        # Launch EC2 instance using Launch Template
        response = ec2.run_instances(
            LaunchTemplate={'LaunchTemplateId': launch_template_id},
            MinCount=1,
            MaxCount=1,
            TagSpecifications=[
                {
                    "ResourceType": "instance",
                    "Tags": [
                        {"Key": INSTANCE_TAG_KEY, "Value": INSTANCE_TAG_VALUE}
                    ],
                }
            ],
        )
        instance_id = response['Instances'][0]['InstanceId']
        logger.info(f"✅ EC2 instance launched: {instance_id}")
        return {"status": "success", "instance_id": instance_id}

    except Exception as e:
        logger.error(f"❌ Failed to launch EC2: {e}")
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    check_csv_and_launch_ec2()

