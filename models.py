from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List
from sqlalchemy.orm import Session

Base = declarative_base()

class Job(Base):
    __tablename__ = "jobs"
    id = Column(Integer, primary_key=True, autoincrement=False)
    job = Column(String, nullable=False)  

class Department(Base):
    __tablename__ = "departments"
    id = Column(Integer, primary_key=True, autoincrement=False)
    department = Column(String, nullable=False) 

class Employee(Base):
    __tablename__ = "hired_employees"
    id = Column(Integer, primary_key=True, autoincrement=False)
    name = Column(String, nullable=False) 
    datetime = Column(String, nullable=False) 
    department_id = Column(Integer, nullable=False) 
    job_id = Column(Integer, nullable=False) 

# 🔹 Pydantic models for validation
class EmployeeSchema(BaseModel):
    id: int = Field(..., gt=0, description="must be a positive integer")
    name: str = Field(..., min_length=1)
    datetime: str  = Field(..., min_length=1)
    department_id: int = Field(..., gt=0, description="must be a positive integer")
    job_id: int = Field(..., gt=0, description="must be a positive integer")

class JobSchema(BaseModel):
    id: int = Field(..., gt=0, description="must be a positive integer")
    job: str = Field(..., min_length=1)

class DepartmentSchema(BaseModel):
    id: int = Field(..., gt=0, description="must be a positive integer")
    department: str = Field(..., min_length=1)

# Generic request schema
class InsertDataRequest(BaseModel):
    employees_data: List[dict]
    jobs_data: List[dict]
    departments_data: List[dict]