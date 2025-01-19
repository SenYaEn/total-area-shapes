"""
@name: poc_s3_feed\send_json_to_s3_bucket.py
@author: jan.strocki@hotmail.co.uk

This module contains a method to send test data to an S3 bucket. This functionality is for demo
and testing purposes only.
"""

# Standard Imports

import json
import os
import time
import boto3

##############################################################################################


def save_records_to_s3_with_delay(data, bucket, folder, delay_seconds=15):
    """
    Saves each record in the data list as a separate JSON file to S3 with a delay between uploads.

    Parameters:
    - data (list): List of dictionaries to save as JSON files.
    - bucket (str): S3 bucket name.
    - folder (str): Folder path in the S3 bucket.
    - delay_seconds (int): Delay in seconds between each upload.
    """

    # Initialize the S3 client
    s3 = boto3.client(
        "s3",
        aws_access_key_id="XXXXXX",
        aws_secret_access_key="XXXXXXXXXXXX",
        region_name="eu-west-2",
    )

    for index, record in enumerate(data):

        # Generate a file name
        file_name = f"{record['type']}_{index + 1}.json"

        # Serialize the record to JSON
        record_json = json.dumps(record)

        # Upload to S3
        s3_key = os.path.join(folder, file_name)
        s3.put_object(Bucket=bucket, Key=s3_key, Body=record_json)

        print(f"Uploaded {file_name} to s3://{bucket}/{s3_key}")

        # Introduce the delay
        print(f"Waiting for {delay_seconds} seconds before the next upload...")
        time.sleep(delay_seconds)


# Save data to S3 with a 15-second delay between uploads
def run_s3_file_upload() -> None:
    data = [
        {"type": "rectangle", "width": 5, "height": 10},
        {"type": "triangle", "base": 2, "height": 3},
        {"type": "circle", "radius": 4},
        {"type": "rectangle", "width": 5, "height": 5},
    ]

    # S3 bucket configuration
    s3_bucket = "plexure-poc"
    s3_folder = "shapes/"

    save_records_to_s3_with_delay(data, s3_bucket, s3_folder, delay_seconds=15)
