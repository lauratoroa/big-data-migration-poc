from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from api.db import SessionLocal
from api.models import BatchInsertDepartments, BatchInsertJobs, BatchInsertHiredEmployees
import pandas as pd

router = APIRouter()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/insert/departments")
def insert_departments(payload: BatchInsertDepartments, db: Session = Depends(get_db)):
    df = pd.DataFrame([d.dict() for d in payload.data])
    df.to_sql("departments", con=db.bind, if_exists="append", index=False)
    return {"message": "Departments inserted successfully!"}

@router.post("/insert/jobs")
def insert_jobs(payload: BatchInsertJobs, db: Session = Depends(get_db)):
    df = pd.DataFrame([d.dict() for d in payload.data])
    df.to_sql("jobs", con=db.bind, if_exists="append", index=False)
    return {"message": "Jobs inserted successfully!"}

@router.post("/insert/hired_employees")
def insert_hired_employees(payload: BatchInsertHiredEmployees, db: Session = Depends(get_db)):
    df = pd.DataFrame([e.dict() for e in payload.data])
    df.to_sql("hired_employees", con=db.bind, if_exists="append", index=False)
    return {"message": "Hired Employees inserted successfully!"}
