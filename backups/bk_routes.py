from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
import pandas as pd
import fastavro
import os
import io
from backups.bk_db import SessionLocal, upload_to_s3, download_from_s3, engine

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Set schemas
TABLE_SCHEMAS = {
    "hired_employees": {
        "type": "record",
        "name": "HiredEmployees",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "name", "type": "string"},
            {"name": "datetime", "type": "string"},
            {"name": "department_id", "type": "int"},
            {"name": "job_id", "type": "int"}
        ]
    },
    "departments": {
        "type": "record",
        "name": "Departments",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "department", "type": "string"}
        ]
    },
    "jobs": {
        "type": "record",
        "name": "Jobs",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "job", "type": "string"}
        ]
    }
}

@router.get("/backup/{table_name}")
def backup_table(table_name: str, db: Session = Depends(get_db)):
    """
    Create a backup table and save it in S3 bucket
    """
    # Check the schema table
    schema = TABLE_SCHEMAS.get(table_name)
    if not schema:
        raise HTTPException(status_code=400, detail=f"Schema for table {table_name} not found")

    # Get data from the table
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, db.bind)

    if df.empty:
        raise HTTPException(status_code=404, detail=f"No data found in table {table_name}")
    
    # Convert datetime field to string
    if "datetime" in df.columns:
        df["datetime"] = df["datetime"].astype(str)

    # Convert DataFrame to a dictionary list
    records = df.to_dict(orient="records")

    # Create a buffer memory to save the AVRO file
    avro_buffer = io.BytesIO()
    fastavro.writer(avro_buffer, schema, records)
    avro_buffer.seek(0)

    # S3 path
    s3_key = f"backups/{table_name}.avro"

    # Call the function to upload the file
    try:
        upload_to_s3(avro_buffer, s3_key)  
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to upload backup to S3: {str(e)}")

    return {"message": f"Backup of {table_name} saved to S3 successfully!", "s3_path": s3_key}

@router.get("/restore/{table_name}")
def restore_table(table_name: str, db: Session = Depends(get_db)):
    """Restore a table from AVRO stored in S3"""
    try:
        # S3 path
        s3_key = f"backups/{table_name}.avro"
        
        # Download file
        avro_buffer = io.BytesIO()
        download_from_s3(s3_key, avro_buffer)
        avro_buffer.seek(0)

        # Read AVRO
        reader = fastavro.reader(avro_buffer)
        records = [record for record in reader]
        
        if not records:
            raise HTTPException(status_code=400, detail=f"No data found in backup for {table_name}.")

        # Convert to DataFrame
        df = pd.DataFrame(records)

        # Inserting in the database
        with db.begin():
            df.to_sql(table_name, con=engine, if_exists="append", index=False)

        return {"message": f"Successfully restored {len(df)} records into {table_name}!"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to restore {table_name}: {str(e)}")
