import boto3
import json
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

def get_secret():
    """Retrieve database credentials from AWS Secrets Manager"""
    secret_name = "big-data-migration-secrets"
    region_name = "us-east-2"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret

# Get database credentials
secrets = get_secret()
s3_bucket = secrets["S3_BUCKET"]  
s3_folder = "backups/"  
S3_BUCKET_NAME = s3_bucket  
 

# database connection
DATABASE_URL = f"postgresql://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}@{secrets['DB_HOST']}:{secrets['DB_PORT']}/{secrets['DB_NAME']}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# AWS S3 Configuration
s3_client = boto3.client("s3")

def upload_to_s3(file_buffer, s3_key: str):
    """Upload a file to an S3 bucket from memory"""
    s3_client.upload_fileobj(file_buffer, S3_BUCKET_NAME, s3_folder + s3_key)

def download_from_s3(s3_key: str, file_path: str):
    """Download a file from S3 to a local path"""
    s3_client.download_file(S3_BUCKET_NAME, s3_folder + s3_key, file_path)
