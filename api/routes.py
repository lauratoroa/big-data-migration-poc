from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from api.db import SessionLocal, validate_dataframe
import pandas as pd

router = APIRouter()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Setting the columns and types
TABLE_VALIDATION_RULES = {
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

@router.post("/insert/{table_name}")
def insert_data(table_name: str, payload: dict, db: Session = Depends(get_db)):
    """
    Inserts data into the table, ensuring it follows data rules
    """

    # Check that data exists in request
    if "data" not in payload or not isinstance(payload["data"], list):
        raise HTTPException(status_code=400, detail="Invalid JSON format. Must contain a 'data' list.")

    data = payload["data"]

    # Batch size limit (1-1000 rows)
    if not (1 <= len(data) <= 1000):
        raise HTTPException(status_code=400, detail="Batch size must be between 1 and 1000 rows.")

    # Convert data to DataFrame
    df = pd.DataFrame(data)

    # Validation rules
    validation_rules = TABLE_VALIDATION_RULES.get(table_name)
    if not validation_rules:
        raise HTTPException(status_code=400, detail=f"No validation rules found for table: {table_name}")

    # Validate data before inserting
    df_valid = validate_dataframe(df, db, table_name, f"/insert/{table_name}")

    if df_valid.empty:
        raise HTTPException(status_code=400, detail=f"All records failed validation. Check error_logs for details")

    # Insert only valid data
    try:
        df_valid.to_sql(table_name, con=db.bind, if_exists="append", index=False)
        return {"message": f"{len(df_valid)} records inserted successfully into {table_name}!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database insertion failed: {str(e)}")

# Endpoints
@router.post("/insert/departments")
def insert_departments(payload: dict, db: Session = Depends(get_db)):
    return insert_data(payload, "departments", db)

@router.post("/insert/jobs")
def insert_jobs(payload: dict, db: Session = Depends(get_db)):
    return insert_data(payload, "jobs", db)

@router.post("/insert/hired_employees")
def insert_hired_employees(payload: dict, db: Session = Depends(get_db)):
    return insert_data(payload, "hired_employees", db)
