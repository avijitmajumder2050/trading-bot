import logging
import boto3
from dhanhq import dhanhq, DhanContext

logging.basicConfig(level=logging.INFO)

AWS_REGION = "ap-south-1"

def get_param(name):
    ssm = boto3.client("ssm", region_name=AWS_REGION)
    return ssm.get_parameter(Name=name, WithDecryption=True)["Parameter"]["Value"]

# -------------------------
# Init Dhan Client
# -------------------------
dhan = dhanhq(
    DhanContext(
        client_id=get_param("/dhan/client_id"),
        access_token=get_param("/dhan/access_token"),
    )
)

# -------------------------
# TEST API
# -------------------------
try:
    response = dhan.get_fund_limits()
    print("✅ get_fund_limits() SUCCESS")
    print(response)

except Exception as e:
    print("❌ get_fund_limits() FAILED")
    print(e)
