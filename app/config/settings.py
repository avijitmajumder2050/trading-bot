# app/config/settings.py

import os
from pytz import timezone
from datetime import time
from botocore.exceptions import ClientError
import boto3

from app.config.aws_ssm import get_param

# ----------------------------
# Timezone
# ----------------------------
IST = timezone("Asia/Kolkata")

# ----------------------------
# Scan Times
# ----------------------------
INSIDEBAR_SCAN_TIME = time(9, 26)

# ----------------------------
# AWS
# ----------------------------
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

s3 = boto3.client("s3", region_name=AWS_REGION)


def get_s3_bucket():
    """
    Auto-detect which bucket exists.
    Priority:
      1. Environment variable S3_BUCKET
      2. new-dhan-trading-data
      3. dhan-trading-data
    """

    candidates = [
        os.getenv("S3_BUCKET"),
        "new-dhan-trading-data",
        "dhan-trading-data",
    ]

    # Remove None and duplicates
    candidates = [b for i, b in enumerate(candidates) if b and b not in candidates[:i]]

    for bucket in candidates:
        try:
            s3.head_bucket(Bucket=bucket)
            print(f"Using S3 Bucket: {bucket}")
            return bucket
        except ClientError:
            pass

    raise RuntimeError("No accessible S3 bucket found.")


S3_BUCKET = get_s3_bucket()

# ----------------------------
# S3 Object Keys
# ----------------------------
MAP_FILE_KEY = "uploads/mapping.csv"
NIFTYMAP_FILE_KEY = "uploads/nifty_mapping.csv"

CANDLE_FILE_KEY = "uploads/inside_bar_15min_data_RS80.csv"
FILTERED_FILE_KEY = "uploads/inside_bar_15min_RS80.csv"
EOD_DATA_PREFIX = "eod_data"

# ----------------------------
# Logs
# ----------------------------
LOG_DIR = "logs"

# =========================
# TELEGRAM (FROM SSM)
# =========================
BOT_TOKEN = get_param("/trading-bot/telegram/BOT_TOKEN", decrypt=True)
CHAT_ID = get_param("/trading-bot/telegram/CHAT_ID")

# ----------------------------
# Telegram Keywords
# ----------------------------
TRIGGER_KEYWORDS = [
    "scanner",
    "scan",
    "momentum",
    "interday",
    "intraday",
]

SWING_KEYWORDS = [
    "swing",
    "position",
]

CROSS_KEYWORDS = [
    "ema cross",
    "cross ema",
    "ema crossover",
    "crossover",
]