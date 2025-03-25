import boto3
import pandas as pd
import json
import psycopg2
from io import StringIO
from sqlalchemy import create_engine

def get_secret():
    """
    Retrieve database and AWS credentials securely from AWS Secrets Manager
    """
    secret_name = "big-data-migration-secrets"
    region_name = "us-east-2"

    # Create the client
    client = boto3.client("secretsmanager", region_name=region_name)

    # Get the secret
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response["SecretString"])

    return secret


def get_s3_client():
    """
    Initialize an S3 client using credentials retrieved from AWS Secrets Manager
    """
    secrets = get_secret()
    return boto3.client(
        "s3",
        aws_access_key_id=secrets["AWS_ACCESS_KEY"],
        aws_secret_access_key=secrets["AWS_SECRET_KEY"],
        region_name=secrets["AWS_REGION"]
    )

def download_csv_from_s3(bucket_name, file_key):
    """
    Download a CSV file from an S3 bucket
    """
    s3_client = get_s3_client()
    # Get file from S3
    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)  
    # Decode file content
    return response["Body"].read().decode("utf-8")  

def load_csv_to_dataframe(csv_data, column_names):
    """
    Convert CSV string data into a Pandas DataFrame with predefined column names
    """
    # Assign column names manually
    return pd.read_csv(StringIO(csv_data), header=None, names=column_names)  

def get_db_connection():
    """
    Create a database connection using credentials from AWS Secrets Manager
    """
    secrets = get_secret()
    
    # Create a PostgreSQL connection string
    engine = create_engine(
        f"postgresql+psycopg2://{secrets['DB_USER']}:{secrets['DB_PASSWORD']}@{secrets['DB_HOST']}:{secrets['DB_PORT']}/{secrets['DB_NAME']}"
    )
    
    return engine

def insert_data_to_db(df, table_name):
    """
    Insert data from a Pandas DataFrame into a PostgreSQL database
    """
    engine = get_db_connection()
    
    # Insert data into the specified table
    df.to_sql(table_name, engine, if_exists="append", index=False)

def main():
    """
    Data import process:
    - Fetch CSV files from S3
    - Convert them into DataFrames with manually defined headers
    - Insert them into the PostgreSQL database
    """
    secrets = get_secret()
    s3_bucket = secrets["S3_BUCKET"]  
    s3_folder = "row-data/"  

    # Define CSV file paths in S3, database table names, and expected column headers
    files = {
        f"{s3_folder}departments.csv": ("departments", ["id", "department"]),
        f"{s3_folder}jobs.csv": ("jobs", ["id", "job"]),
        f"{s3_folder}hired_employees.csv": ("hired_employees", ["id", "name", "datetime", "department_id", "job_id"])
    }

    for file_key, (table_name, column_names) in files.items():
        print(f"Processing {file_key} Table: {table_name}")

        # Step 1: Download CSV from S3
        csv_data = download_csv_from_s3(s3_bucket, file_key)
        
        # Step 2: Load CSV into a DataFrame with explicit column names
        df = load_csv_to_dataframe(csv_data, column_names)

        # Step 3: Insert data into PostgreSQL
        insert_data_to_db(df, table_name)

        print(f"{file_key} imported successfully into {table_name}")

if __name__ == "__main__":
    main()
