import boto3
import json
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def get_secret():
    """
    Retrieve database credentials securely from AWS Secrets Manager
    """
    secret_name = "big-data-migration-secrets"
    region_name = "us-east-2"

    client = boto3.client("secretsmanager", region_name=region_name)
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])
    return secret

# Get database credentials
secrets = get_secret()

# Create database connection
DATABASE_URL = f"postgresql://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}@{secrets['DB_HOST']}:{secrets['DB_PORT']}/{secrets['DB_NAME']}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
