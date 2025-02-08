import pandas as pd
import io
import fastavro
import os
import csv
import logging
from fastapi import FastAPI, Depends, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session
from database import get_db_connection
from models import *
from pydantic import ValidationError
from typing import Type
from sqlalchemy.exc import IntegrityError,SQLAlchemyError
from sqlalchemy import text,MetaData,Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.automap import automap_base
from decimal import Decimal

app = FastAPI()

# *********** GENERAL FUNCTIONS AND DEFINITIONS *********** 
# Configuration of log
logging.basicConfig(filename="error_log.txt", level=logging.ERROR, format="%(asctime)s - %(message)s")
# getting connection
engine, SessionLocal = get_db_connection()
db = SessionLocal()
# Creating backup folder
BACKUP_DIR = "backups"
os.makedirs(BACKUP_DIR, exist_ok=True)

# Generalized function to validate the data using a given Pydantic model
async def validate_rows(row_dict: dict, schema: Type[BaseModel], basemodel: Type,valid_records: list, error_count: int):
    try:
        transaction = schema(**row_dict)  # Validation Pydantic
        # Convert Pydantic schema to SQLAlchemy model
        record = basemodel(**transaction.dict())
        valid_records.append(record)
    except ValidationError as e:
        error_count += 1
        logging.error(f"Validation error in row {row_dict}: {e}")
    return error_count

def SavingDatainSql(valid_records: list, table:String):
    # Perform UPSERT (Update if exists, Insert if not)    
    for record in valid_records:
        row_dict = record.__dict__.copy()
        row_dict.pop("_sa_instance_state", None)  # Remove SQLAlchemy internal field)
        sql = f"""
        MERGE INTO {table} AS target
        USING (SELECT :id AS id) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET {', '.join([f'{key} = :{key}' for key in row_dict.keys() if key != 'id'])}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(row_dict.keys())})
            VALUES ({', '.join([f':{key}' for key in row_dict.keys()])});
        """

        try:
            db.execute(text(sql), row_dict)
        except SQLAlchemyError as e:
            error_count += 1
            logging.error(f"Database error during UPSERT: {e}")

    db.commit() 

#  *********** CHALLENGE 1 - NUMBER 1 .To upload csvs: jobs.csv or departments.csv or hired_employees.csv with validations *********** 
@app.post("/UploadCsvDB/")
async def upload_csv_files(file: UploadFile = File(...)):

    valid_records = []
    error_count = 0
    contents = await file.read()
    decoded = contents.decode('utf-8').splitlines()
    reader = csv.reader(decoded)
    
    # Default empty values
    CSV_HEADERS = []
    table = ""
    schema = None
    basemodel = None

    # Determine the schema and table based on file name
    if file.filename == "hired_employees.csv":
        CSV_HEADERS = ["id", "name", "datetime", "department_id", "job_id"]
        table = "hired_employees"
        schema = EmployeeSchema
        basemodel = Employee
    elif file.filename == "jobs.csv":
        CSV_HEADERS = ["id", "job"]
        table = "jobs"
        schema = JobSchema
        basemodel = Job
    elif file.filename == "departments.csv":
        CSV_HEADERS = ["id", "department"]
        table = "departments"
        schema = DepartmentSchema
        basemodel = Department
    else:
        raise HTTPException(status_code=400, detail="Invalid table name")

    # Process each row in CSV
    for row in reader:
        row_dict = dict(zip(CSV_HEADERS, row))
        # Validate rows
        error_count = await validate_rows(row_dict, schema,basemodel,valid_records, error_count)

    SavingDatainSql(valid_records,table)
    
    return {
        "message": "************** Processed Rows ***************",
        "valid_records": len(valid_records),
        "errors": error_count
    }
#  *********** CHALLENGE 1 - NUMBER 2 .To receive new data *********** 
@app.post("/InsertDataDB/")
async def insert_data(request: InsertDataRequest):
    # not empty
    if not (len(request.employees_data) > 0 or len(request.jobs_data) > 0 or len(request.departments_data) > 0):
        raise HTTPException(status_code=400, detail="No data provided")
    #not higuer than 1000
    if len(request.employees_data) > 1000:
        raise HTTPException(status_code=400, detail="Employees data batch size exceeds 1000 records")
    
    if len(request.jobs_data) > 1000:
        raise HTTPException(status_code=400, detail="Jobs data batch size exceeds 1000 records")
    
    if len(request.departments_data) > 1000:
        raise HTTPException(status_code=400, detail="Departments data batch size exceeds 1000 records")

    try:
        valid_records_employees = []
        valid_records_jobs = []
        valid_records_departments = []
        error_count = 0

        # Processing employee
        if request.employees_data:
            for row in request.employees_data:
                try:
                    row_dict = EmployeeSchema(**row)
                    row_dict_dump = row_dict.model_dump()
                    error_count = await validate_rows(row_dict_dump, EmployeeSchema, Employee, valid_records_employees, error_count)
                except Exception as e:
                    print(f"Error to process employee: {e}") 

            if valid_records_employees:
                SavingDatainSql(valid_records_employees, "hired_employees")

        # Processing jobs
        if request.jobs_data:
            for row in request.jobs_data:
                try:
                    row_dict = JobSchema(**row)
                    row_dict_dump = row_dict.model_dump()
                    error_count = await validate_rows(row_dict_dump, JobSchema, Job, valid_records_jobs, error_count)
                except Exception as e:
                    print(f"Error to process job: {e}") 

            if valid_records_jobs:
                SavingDatainSql(valid_records_jobs, "jobs")
        # Processing departments
        if request.departments_data:
            for row in request.departments_data:
                try:
                    row_dict = DepartmentSchema(**row)
                    row_dict_dump = row_dict.model_dump()
                    error_count = await validate_rows(row_dict_dump, DepartmentSchema, Department, valid_records_departments, error_count)
                except Exception as e:
                    print(f"Error to process department: {e}") 

            if valid_records_jobs:
                SavingDatainSql(valid_records_jobs, "jobs")

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail="Validation failed: " + str(e))
    finally:
        db.close()

    return {
        "message": "************** Processed Rows ***************",
        "valid_records_employees": len(valid_records_employees),
        "valid_records_jobs": len(valid_records_jobs),
        "valid_records_departments": len(valid_records_departments),
        "errors": error_count
    }
#  *********** CHALLENGE 1 - NUMBER 3 .To create AVRO backup *********** 
def get_table_names():
    result = db.execute(text("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'"))
    return [row[0] for row in result.fetchall()]  

def map_sqlalchemy_to_avro(sql_type):
    """Mapea los tipos de SQLAlchemy a tipos de AVRO."""
    sql_type_str = str(sql_type).lower()
    
    if "int" in sql_type_str:
        return "int"
    elif "float" in sql_type_str or "decimal" in sql_type_str or "numeric" in sql_type_str:
        return "float"
    elif "char" in sql_type_str or "text" in sql_type_str or "varchar" in sql_type_str:
        return "string"
    elif "date" in sql_type_str or "time" in sql_type_str or "datetime" in sql_type_str:
        return "string"  # AVRO no tiene tipo DATE, se almacena como string
    elif "bool" in sql_type_str:
        return "boolean"
    else:
        return "string"  # Tipo por defecto
    
def convert_value(value, avro_type):
    if value is None:
        return "" if avro_type == "string" else 0
    if avro_type == "int":
        return int(value) if isinstance(value, (int, float, Decimal)) else 0
    if avro_type == "float":
        return float(value) if isinstance(value, (int, float, Decimal)) else 0.0
    if avro_type == "boolean":
        return bool(value)
    return str(value) 
 
def backup_table(db, table_name):
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    Base1 = automap_base(metadata=metadata)
    Base1.prepare()    
    table = Base1.classes.get(table_name)
    if not table:
        print(f"Table{table_name} not found")
        return
    # Getting tables
    data = db.query(table).all()
    if not data:
        print(f"Empty table: {table_name}")
        return
    records = []
    for row in data:
        record = {}
        for col in table.__table__.columns:
            avro_type = map_sqlalchemy_to_avro(col.type)
            value = getattr(row, col.name)
            record[col.name] = convert_value(value, avro_type)  # Converting values
        records.append(record)

    # Creating AVRO schema dynamically
    schema = {
        "type": "record",
        "name": table_name,
        "fields": [{"name": col.name, "type": map_sqlalchemy_to_avro(col.type)} for col in table.__table__.columns]

    }
    file_path = os.path.join(BACKUP_DIR, f"{table_name}_backup_.avro")    
    with open(file_path, "wb") as f:
        fastavro.writer(f, schema, records)

@app.get("/BackupTablesAvro")
def backup_all_tables():
    table_names = get_table_names(db)
    for table_name in table_names:
        backup_table(db, table_name)

    return {"message": "Backup done!"}
#  *********** CHALLENGE 1 - NUMBER 4 .To restore AVRO backup  ***********
def restore_table(table_name):
    file_path = os.path.join(BACKUP_DIR, f"{table_name}_backup_.avro")
    
    if not os.path.exists(file_path):
        print(f"Backup file for table {table_name} not found.")
        return
    
    # Load schema and data from AVRO
    with open(file_path, "rb") as f:
        reader = fastavro.reader(f)
        records = [record for record in reader]
    
    if not records:
        print(f"No data found in the backup file: {file_path}")
        return
    
    metadata = MetaData()
    metadata.reflect(bind=engine, only=[table_name])
    table = Table(table_name, metadata, autoload_with=engine)

    
    try:
        # Insert records into the table, deleting first
        db.execute(text(f"TRUNCATE TABLE {table_name}"))
        db.execute(table.insert(), records)
        db.commit()
        print(f"Successfully restored {len(records)} records into {table_name}.")
    except Exception as e:
        db.rollback()
        print(f"Error restoring table {table_name}: {e}")
    finally:
        db.close()

@app.post("/RestoreTableAvro")
def restore_table(table_name: str):
    try:
        restore_table(table_name)
        return {"message": f"Table {table_name} restored successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
#  *********** CHALLENGE 2 - NUMBER 1 .To restore AVRO backup  ***********
# Number of employees hired for each job and department in 2021 divided by quarter. The table must be ordered alphabetically by department and job.
@app.get("/hired-employees-by-quarter", response_class=HTMLResponse)
def get_hired_employees_by_quarter():
    query = text("""
        SELECT 
            department,
            job,
            ISNULL([1], 0) AS Q1,
            ISNULL([2], 0) AS Q2,
            ISNULL([3], 0) AS Q3,
            ISNULL([4], 0) AS Q4
        FROM (
            SELECT 
                d.department,
                j.job,
                DATEPART(QUARTER, h.[datetime]) AS quarter,
                COUNT(*) AS total_hired
            FROM hired_employees h inner join jobs j
					on h.job_id = j.id
				 inner join departments d
					on h.department_id = d.id
            WHERE YEAR(h.[datetime]) = 2021
            GROUP BY department, job, DATEPART(QUARTER, h.[datetime])
        ) AS source_table
        PIVOT (
            SUM(total_hired) FOR quarter IN ([1], [2], [3], [4])
        ) AS pivot_table
        ORDER BY 1,2;
    """)

    result = db.execute(query)

    # Generate the HTML table
    table_html = """
    <html>
    <head>
        <title>Hired Employees by Quarter</title>
        <style>
            table { 
                width: 100%%; 
                border-collapse: collapse; 
                font-family: Arial, sans-serif; 
            }
            th, td { 
                border: 1px solid black; 
                padding: 8px; 
                text-align: left; 
            }
            th { 
                background-color: #f2f2f2; 
            }
        </style>
    </head>
    <body>
        <h2>Hired Employees by Quarter (2021)</h2>
        <table>
            <tr>
                <th>Department</th>
                <th>Job</th>
                <th>Q1</th>
                <th>Q2</th>
                <th>Q3</th>
                <th>Q4</th>
            </tr>
    """

    for row in result:
        table_html += f"""
            <tr>
                <td>{row[0]}</td>
                <td>{row[1]}</td>
                <td>{row[2]}</td>
                <td>{row[3]}</td>
                <td>{row[4]}</td>
                <td>{row[5]}</td>
            </tr>
        """

    table_html += """
        </table>
    </body>
    </html>
    """

    return HTMLResponse(content=table_html)
#  *********** CHALLENGE 1 - NUMBER 2 .To restore AVRO backup  ***********
#List of ids, name and number of employees hired of each department that hired more employees than the mean of employees hired in 2021 
@app.get("/departments-above-mean", response_class=HTMLResponse)
def get_departments_above_mean():
    query = text("""
        WITH DepartmentHires AS (
            SELECT 
                d.id,
                d.department,
                COUNT(*) AS hired
            FROM hired_employees h inner join departments d
				on h.department_id = d.id
            WHERE YEAR([datetime]) = 2021
            GROUP BY d.id, d.department
        ),
        MeanHires AS (
            SELECT AVG(hired) AS avg_hired
            FROM DepartmentHires
        )
        SELECT 
            dh.id,
            dh.department,
            dh.hired
        FROM DepartmentHires dh
        JOIN MeanHires mh ON dh.hired > mh.avg_hired
        ORDER BY dh.hired DESC
    """)

    result = db.execute(query)

    # Start building the HTML table
    table_html = """
    <html>
    <head>
        <title>Departments Above Mean Hiring</title>
        <style>
            body {
                font-family: Arial, sans-serif;
                margin: 20px;
                text-align: center;
            }
            table {
                width: 60%;
                margin: auto;
                border-collapse: collapse;
                box-shadow: 0px 0px 10px #aaa;
            }
            th, td {
                border: 1px solid black;
                padding: 10px;
                text-align: center;
            }
            th {
                background-color: #4CAF50;
                color: white;
            }
            tr:nth-child(even) {
                background-color: #f2f2f2;
            }
        </style>
    </head>
    <body>
        <h2>Departments That Hired More Employees Than the Mean (2021)</h2>
        <table>
            <tr>
                <th>Id</th>
                <th>Department</th>
                <th>Hired</th>
            </tr>
    """

    for row in result:
        table_html += f"""
            <tr>
                <td>{row[0]}</td>
                <td>{row[1]}</td>
                <td>{row[2]}</td>
            </tr>
        """

    table_html += """
        </table>
    </body>
    </html>
    """

    return HTMLResponse(content=table_html)
