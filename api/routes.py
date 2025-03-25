import logging
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from api.db import SessionLocal
from api.models import BatchInsertDepartments, BatchInsertJobs, BatchInsertHiredEmployees
import pandas as pd

router = APIRouter()

logging.basicConfig(filename="invalid_records.log", level=logging.WARNING, format="%(asctime)s - %(message)s")

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def validate_dataframe(df, table_name):
    """
    Validates that all required fields are present
    Logs invalid rows and removes them from the dataframe
    """
    required_columns = {
        "departments": ["id", "department"],
        "jobs": ["id", "job"],
        "hired_employees": ["id", "name", "datetime", "department_id", "job_id"]
    }

    if table_name not in required_columns:
        raise ValueError(f"Unknown table: {table_name}")

    missing_columns = [col for col in required_columns[table_name] if col not in df.columns]
    if missing_columns:
        raise HTTPException(status_code=400, detail=f"Missing columns: {missing_columns}")

    invalid_rows = df[df.isnull().any(axis=1)]
    if not invalid_rows.empty:
        logging.warning(f"Invalid records found in {table_name}:\n{invalid_rows}")
        # Removes nulls
        df = df.dropna()  

    return df

@router.post("/insert/departments")
def insert_departments(payload: BatchInsertDepartments, db: Session = Depends(get_db)):
    df = pd.DataFrame([d.dict() for d in payload.data])
    df = validate_dataframe(df, "departments")
    if df.empty:
        return {"message": "No valid records to insert"}
    df.to_sql("departments", con=db.bind, if_exists="append", index=False)
    return {"message": "Departments inserted successfully!"}

@router.post("/insert/jobs")
def insert_jobs(payload: BatchInsertJobs, db: Session = Depends(get_db)):
    df = pd.DataFrame([d.dict() for d in payload.data])
    df = validate_dataframe(df, "jobs")
    if df.empty:
        return {"message": "No valid records to insert"}
    df.to_sql("jobs", con=db.bind, if_exists="append", index=False)
    return {"message": "Jobs inserted successfully!"}

@router.post("/insert/hired_employees")
def insert_hired_employees(payload: BatchInsertHiredEmployees, db: Session = Depends(get_db)):
    df = pd.DataFrame([e.dict() for e in payload.data])
    df = validate_dataframe(df, "hired_employees")
    if df.empty:
        return {"message": "No valid records to insert"}
    df.to_sql("hired_employees", con=db.bind, if_exists="append", index=False)
    return {"message": "Hired Employees inserted successfully!"}
