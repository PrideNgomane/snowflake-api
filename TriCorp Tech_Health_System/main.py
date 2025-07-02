from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
import pandas as pd

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fetch_health_data():
    conn = snowflake.connector.connect(
        user="NGOMANE01",
        password="P.Ngomane@2025",
        account="KHWZLWE-TY26316",
        warehouse="COMPUTE_WH",
        database="TRICORE_TECH",
        schema="PUBLIC"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM HEALTH_SECTOR_INITIATIVES;")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=columns)
    return df

@app.get("/health-data")
def get_health_sector_data():
    df = fetch_health_data()
    return df.to_dict(orient="records")
