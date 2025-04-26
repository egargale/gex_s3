import os
import time
from dotenv import load_dotenv
import boto3
from pathlib import Path

# Load environment variables
load_dotenv()

# Retrieve environment variables with proper error handling
def get_env_variable(key, error_message):
    value = os.environ.get(key)
    if not value:
        raise ValueError(error_message)
    return value

AWS_ACCESS_KEY_ID = get_env_variable('AWS_ACCESS_KEY_ID', 'AWS_ACCESS_KEY_ID is missing')
AWS_SECRET_ACCESS_KEY = get_env_variable('AWS_SECRET_ACCESS_KEY', 'AWS_SECRET_ACCESS_KEY is missing')
S3_ENDPOINT_URL = get_env_variable('S3_ENDPOINT_URL', 'S3_ENDPOINT_URL is missing')
S3_BUCKET = get_env_variable('S3_BUCKET', 'S3_BUCKET is missing')

# Initialize S3 client
s3 = boto3.client(
    's3',
    endpoint_url=S3_ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Define file path using pathlib for cross-platform compatibility
file_path = Path("figure") / "gexbysurface_SPX_20250423_3.png"
bucket_name = S3_BUCKET
key_name = "test.png"

# Upload file to S3
with open(file_path, 'rb') as file_data:
    s3.put_object(Body=file_data, Bucket=bucket_name, Key=key_name)

# Helper function to wait for an object to exist with a timeout
def wait_for_object(s3_client, bucket, key, action, timeout=30):
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            print(f"Object is now available for {action}.")
            return
        except s3_client.exceptions.NoSuchKey:
            time.sleep(1)
            print(f"Waiting for object to be {action}...")
    raise TimeoutError(f"Timeout while waiting for object to be {action}.")

# Wait for the object to be uploaded
wait_for_object(s3, bucket_name, key_name, "uploaded")

# Download the object from S3
s3.get_object(Bucket=S3_BUCKET, Key=key_name)

# Wait for the object to be downloaded
wait_for_object(s3, bucket_name, key_name, "downloaded")