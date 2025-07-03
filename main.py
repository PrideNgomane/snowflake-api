from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import snowflake.connector
import pandas as pd

# Define the patient data model
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
    SA_ID: str

# Initialize FastAPI app
app = FastAPI(
    title="Snowflake API",
    description="API to fetch and add patient data from/to Snowflake",
    version="1.0"
)

# Enable CORS for frontend access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change to your frontend URL in prod
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_snowflake_conn():
    return snowflake.connector.connect(
        user="NGOMANE01",
        password="P.Ngomane@2025",
        account="KHWZLWE-TY26316",
        warehouse="COMPUTE_WH",
        database="TRICORE_TECH",
        schema="PUBLIC"
    )

@app.get("/")
def root():
    return {"message": "Snowflake API live. Use /health-data to GET, /add-patient to POST."}

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

@app.post("/add-patient")
def add_patient_to_snowflake(patient: Patient):
    conn = get_snowflake_conn()
    try:
        cursor = conn.cursor()
        sql = f"""
        INSERT INTO HEALTH_SECTOR_INITIATIVES (
            PATIENT_ID, NAME, AGE, DISEASES, PHONE, LANGUAGE, APPOINTMENT_DATE,
            CLINIC_VISITS, NEXT_VISIT, NEXT_VISIT_TIME, LAST_VISIT, LAST_VISIT_TIME,
            CLINIC_NAME, LOCATION, CREATED_DATE, CREATED_TIME, GENDER, SA_ID
        )
        VALUES (
            '{patient.PATIENT_ID}', '{patient.NAME}', {patient.AGE}, '{patient.DISEASES}',
            '{patient.PHONE}', '{patient.LANGUAGE}', '{patient.APPOINTMENT_DATE}',
            {patient.CLINIC_VISITS}, '{patient.NEXT_VISIT}', '{patient.NEXT_VISIT_TIME}',
            '{patient.LAST_VISIT}', '{patient.LAST_VISIT_TIME}', '{patient.CLINIC_NAME}',
            '{patient.LOCATION}', '{patient.CREATED_DATE}', '{patient.CREATED_TIME}',
            '{patient.GENDER}', '{patient.SA_ID}'
        );
        """
        cursor.execute(sql)
        return {"message": "Patient added successfully"}
    except Exception as e:
        return {"error": str(e)}
    finally:
        cursor.close()
        conn.close()
