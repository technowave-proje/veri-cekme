import os
from fastapi import FastAPI
from fetch_tempo import fetch_and_store
from apscheduler.schedulers.background import BackgroundScheduler
import mysql.connector
from pydantic import BaseModel

app = FastAPI()
scheduler = BackgroundScheduler()

scheduler.add_job(fetch_and_store, 'cron', minute=0)
scheduler.start()

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": os.environ.get("MYSQL_PASSWORD"),
    "database": "tempo_data_deneme"  # test veritabanı
}

class DataRequest(BaseModel):
    start_time: str
    end_time: str

@app.get("/latest_no2")
def latest_no2():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tempo_no2 ORDER BY timestamp DESC LIMIT 1000")
    rows = cursor.fetchall()
    conn.close()
    return {"data": rows}

@app.post("/no2_range")
def no2_range(req: DataRequest):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    query = "SELECT * FROM tempo_no2 WHERE timestamp BETWEEN %s AND %s"
    cursor.execute(query, (req.start_time, req.end_time))
    rows = cursor.fetchall()
    conn.close()
    return {"data": rows}
