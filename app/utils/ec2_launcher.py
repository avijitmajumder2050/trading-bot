import boto3
import logging

logger = logging.getLogger(__name__)
# ── Config ──
BUCKET = "dhan-trading-data"
CSV_KEY = "uploads/nifty_15m_breakout_signals.csv"
EC2_REGION = "ap-south-1"

# SSM Parameter where Launch Template ID is stored
SSM_PARAM_NAME = "/trading-bot-algo/ec2/launch_template_id"



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
    Reads CSV from S3, checks number of rows, launches EC2 if >= 3 rows.
    """
    s3 = boto3.client("s3")
    ec2 = boto3.client("ec2", region_name=EC2_REGION)

    launch_template_id = get_launch_template_id_from_ssm()
    if not launch_template_id:
        return {"status": "failed", "error": "Launch Template ID not found"}

    try:
        # Read CSV from S3
        obj = s3.get_object(Bucket=BUCKET, Key=CSV_KEY)
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
            MaxCount=1
        )
        instance_id = response['Instances'][0]['InstanceId']
        logger.info(f"✅ EC2 instance launched: {instance_id}")
        return {"status": "success", "instance_id": instance_id}

    except Exception as e:
        logger.error(f"❌ Failed to launch EC2: {e}")
        return {"status": "failed", "error": str(e)}


if __name__ == "__main__":
    check_csv_and_launch_ec2()

