# app/utils/save_insidebar_breakout.py

import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from botocore.exceptions import ClientError

BUCKET = "dhan-trading-data"
KEY = "uploads/fyer_insiderbar_brekout.csv"

s3 = boto3.client("s3")

def save_insidebar_breakout(hit):
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        obj = s3.get_object(Bucket=BUCKET, Key=KEY)
        df = pd.read_csv(obj["Body"])
    except ClientError:
        df = pd.DataFrame()

    new_row = {
        "trade_date": today,
        "time": datetime.now().strftime("%H:%M:%S"),
        "stock_name": hit["Stock Name"],
        "security_id": hit["Security ID"],
        "price": hit["Price"],
        "entry": hit["Entry"],
        "sl": hit["SL"],
        "qty": hit["Quantity"],
        "risk": hit["Expected Loss"]
    }

    if df.empty:
        df = pd.DataFrame([new_row])

    else:
        # new day → overwrite
        if str(df.iloc[0]["trade_date"]) != today:
            df = pd.DataFrame([new_row])

        else:
            df = pd.concat(
                [df, pd.DataFrame([new_row])],
                ignore_index=True
            )

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=BUCKET,
        Key=KEY,
        Body=csv_buffer.getvalue()
    )