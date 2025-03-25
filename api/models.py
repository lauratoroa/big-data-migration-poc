from pydantic import BaseModel, Field
from typing import List
from datetime import datetime

class Department(BaseModel):
    id: int
    name: str

class Job(BaseModel):
    id: int
    name: str

class HiredEmployee(BaseModel):
    id: int
    name: str
    datetime: datetime
    department_id: int
    job_id: int

# Models for batch insert
class BatchInsertDepartments(BaseModel):
    data: List[Department]

class BatchInsertJobs(BaseModel):
    data: List[Job]

class BatchInsertHiredEmployees(BaseModel):
    data: List[HiredEmployee]
