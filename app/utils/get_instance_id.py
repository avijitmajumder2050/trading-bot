#!/usr/bin/env python3
import requests

def get_instance_id():
    try:
        # Get IMDSv2 token
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"}
        ).text

        # Get instance ID
        instance_id = requests.get(
            "http://169.254.169.254/latest/meta-data/instance-id",
            headers={"X-aws-ec2-metadata-token": token}
        ).text

        return instance_id

    except Exception as e:
        print(f"Error fetching instance ID: {e}")
        return "UNKNOWN"

if __name__ == "__main__":
    print(get_instance_id())

