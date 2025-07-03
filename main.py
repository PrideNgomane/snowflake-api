from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import snowflake.connector
import pandas as pd
from datetime import datetime, timedelta
import random

# 🎯 Patient data model
class Patient(BaseModel):
    PATIENT_ID: str
    NAME: str
    AGE: int
    DISEASES: str
    PHONE: str
    LANGUAGE: str
    APPOINTMENT_DATE: str
    CLINIC_VISITS: int
    NEXT_VISIT: str
    NEXT_VISIT_TIME: str
    LAST_VISIT: str
    LAST_VISIT_TIME: str
    CLINIC_NAME: str
    LOCATION: str
    CREATED_DATE: str
    CREATED_TIME: str
    GENDER: str
    SA_ID: str = None  # Optional, generate if not provided

# 🧠 SA ID generator
def luhn_checksum(id_without_checksum):
    digits = [int(d) for d in id_without_checksum]
    odd_sum = sum(digits[-1::-2])
    even_digits = digits[-2::-2]
    even_sum = 0
    for d in even_digits:
        doubled = d * 2
        even_sum += doubled if doubled < 10 else doubled - 9
    total = odd_sum + even_sum
    return (10 - (total % 10)) % 10

def generate_sa_id(age, gender):
    birth_year = datetime.today().year - age
    birth_date = datetime(birth_year, random.randint(1, 12), random.randint(1, 28))
    yy, mm, dd = birth_date.strftime("%y"), birth_date.strftime("%m"), birth_date.strftime("%d")
    seq = random.randint(5000, 9999) if gender.upper().startswith('M') else random.randint(0, 4999)
    partial = f"{yy}{mm}{dd}{seq:04d}08"
    return partial + str(luhn_checksum(partial))

# 🔧 FastAPI setup
app = FastAPI(
    title="Snowflake API",
    description="API to fetch and add patient data from/to Snowflake",
    version="1.0"
)

# 🌍 CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 🔌 Snowflake connector
def get_snowflake_conn():
    return snowflake.connector.connect(
        user="NGOMANE01",
        password="P.Ngomane@2025",
        account="KHWZLWE-TY26316",
        warehouse="COMPUTE_WH",
        database="TRICORE_TECH",
        schema="PUBLIC"
    )

# 👋 Root route
@app.get("/")
def root():
    return {"message": "Snowflake API live. Use /health-data to GET, /add-patient to POST."}

# 📥 Fetch from Snowflake
@app.get("/health-data")
def get_health_sector_data():
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM HEALTH_SECTOR_INITIATIVES;")
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        df = pd.DataFrame(rows, columns=columns)
        return df.to_dict(orient="records")
    finally:
        cursor.close()
        conn.close()

# 📤 Add new patient
@app.post("/add-patient")
def add_patient_to_snowflake(patient: Patient):
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()

        # Auto-generate SA ID if not provided
        if not patient.SA_ID:
            patient.SA_ID = generate_sa_id(patient.AGE, patient.GENDER)

        sql = f"""
        INSERT INTO HEALTH_SECTOR_INITIATIVES (
            PATIENT_ID, NAME, AGE, DISEASES, PHONE, LANGUAGE, APPOINTMENT_DATE,
            CLINIC_VISITS, NEXT_VISIT, NEXT_VISIT_TIME, LAST_VISIT, LAST_VISIT_TIME,
            CLINIC_NAME, LOCATION, CREATED_DATE, CREATED_TIME, GENDER, SA_ID
        ) VALUES (
            '{patient.PATIENT_ID}', '{patient.NAME}', {patient.AGE}, '{patient.DISEASES}',
            '{patient.PHONE}', '{patient.LANGUAGE}', '{patient.APPOINTMENT_DATE}',
            {patient.CLINIC_VISITS}, '{patient.NEXT_VISIT}', '{patient.NEXT_VISIT_TIME}',
            '{patient.LAST_VISIT}', '{patient.LAST_VISIT_TIME}', '{patient.CLINIC_NAME}',
            '{patient.LOCATION}', '{patient.CREATED_DATE}', '{patient.CREATED_TIME}',
            '{patient.GENDER}', '{patient.SA_ID}'
        );
        """
        cursor.execute(sql)
        return {"message": "Patient added successfully", "SA_ID": patient.SA_ID}
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()
