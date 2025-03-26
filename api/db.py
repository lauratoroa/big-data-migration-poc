import boto3
import json
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session

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

# Column definitions for validation
TABLE_SCHEMAS = {
    "departments": {
        "required_columns": ["id", "department"],
        "expected_types": {"id": int, "department": str}
    },
    "jobs": {
        "required_columns": ["id", "job"],
        "expected_types": {"id": int, "job": str}
    },
    "hired_employees": {
        "required_columns": ["id", "name", "datetime", "department_id", "job_id"],
        "expected_types": {"id": int, "name": str, "datetime": str, "department_id": int, "job_id": int}
    }
}


def validate_dataframe(df: pd.DataFrame, db: Session, table_name: str, endpoint: str):
    """
    Validates a DataFrame before inserting it into the database
    - Ensures required columns exist
    - Checks for missing values
    - Validates data types
    - Logs invalid rows in the database
    """
    if table_name not in TABLE_SCHEMAS:
        raise ValueError(f"Unknown table: {table_name}")

    schema = TABLE_SCHEMAS[table_name]
    required_columns = schema["required_columns"]
    expected_types = schema["expected_types"]

    errors = []
    valid_rows = []

    for _, row in df.iterrows():
        row_errors = []

        # Check for missing values
        for col in required_columns:
            if col not in row or pd.isnull(row[col]):
                row_errors.append(f"Missing value in column: {col}")

        # Check data types
        for col, expected_type in expected_types.items():
            if col in row and not isinstance(row[col], expected_type):
                row_errors.append(f"Invalid type for column {col}: Expected {expected_type}, got {type(row[col])}")

        if row_errors:
            error_message = "; ".join(row_errors)
            errors.append({"endpoint": endpoint, "error_message": error_message})
        else:
            valid_rows.append(row)

    # Save errors in error_logs
    if errors:
        try:
            query = text("INSERT INTO error_logs (endpoint, error_message) VALUES (:endpoint, :error_message)")
            for error in errors:
                db.execute(query, error)
            db.commit()
        except Exception as e:
            print(f"Failed to log validation errors: {e}")

    return pd.DataFrame(valid_rows)
