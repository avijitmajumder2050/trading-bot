# app/utils/save_insidebar_breakout.py

import boto3
import pandas as pd
import logging
from io import StringIO
from datetime import datetime
from botocore.exceptions import ClientError

from app.config.settings import IST

BUCKET = "dhan-trading-data"
KEY = "uploads/fyer_insiderbar_brekout.csv"

s3 = boto3.client("s3")


def save_insidebar_breakout(hit):
    """
    Save InsideBar breakout to S3 CSV.

    Features:
    - One CSV file in S3
    - Auto reset on new trading day
    - Prevent duplicate stock entries
    - Store SL%
    """

    today = datetime.now(IST).strftime("%Y-%m-%d")
    current_time = datetime.now(IST).strftime("%H:%M:%S")

    try:
        obj = s3.get_object(
            Bucket=BUCKET,
            Key=KEY
        )
        df = pd.read_csv(obj["Body"])

    except ClientError:
        df = pd.DataFrame()

    sl_pct = round(
        ((hit["Entry"] - hit["SL"]) / hit["Entry"]) * 100,
        2
    )

    new_row = {
        "trade_date": today,
        "time": current_time,
        "stock_name": hit["Stock Name"],
        "security_id": hit["Security ID"],
        "price": hit["Price"],
        "entry": hit["Entry"],
        "sl": hit["SL"],
        "sl_pct": sl_pct,
        "qty": hit["Quantity"],
        "risk": hit["Expected Loss"]
    }

    # First record
    if df.empty:
        df = pd.DataFrame([new_row])

    else:

        # New day -> reset CSV
        file_date = str(df.iloc[0]["trade_date"])

        if file_date != today:
            logging.info(
                f"📅 New trading day detected. "
                f"Resetting {KEY}"
            )
            df = pd.DataFrame([new_row])

        else:

            # Skip duplicate stock
            if (
                df["stock_name"]
                .astype(str)
                .str.upper()
                .eq(hit["Stock Name"].upper())
                .any()
            ):
                logging.info(
                    f"⏭️ {hit['Stock Name']} already exists in CSV"
                )
                return

            df = pd.concat(
                [df, pd.DataFrame([new_row])],
                ignore_index=True
            )

    # Rank by lowest SL%
    df = df.sort_values(
        by="sl_pct",
        ascending=True
    ).reset_index(drop=True)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=csv_buffer.getvalue()
    )

    logging.info(
        f"💾 Saved {hit['Stock Name']} "
        f"(SL%={sl_pct}) to s3://{BUCKET}/{KEY}"
    )