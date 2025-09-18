import datetime as dt
import os
from harmony import BBox, Client, Collection, Request
import xarray as xr
import mysql.connector
from dotenv import load_dotenv

load_dotenv()

USERNAME = os.getenv("EARTHDATA_USER")
PASSWORD = os.getenv("EARTHDATA_PASS")

harmony_client = Client(auth=(USERNAME, PASSWORD))

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": os.environ.get("MYSQL_PASSWORD"),
    "database": "tempo_data_deneme"  # test veritabanı
}

def fetch_and_store():
    now = dt.datetime.utcnow()
    start_time = now - dt.timedelta(hours=1)  # Son 1 saatteki veri
    stop_time = now

    request = Request(
        collection=Collection(id="C2930763263-LARC_CLOUD"),  # TEMPO NO2 L3
        temporal={"start": start_time, "stop": stop_time},
        spatial=BBox(-170, 10, -10, 80)  # Kuzey Amerika ve çevresi
    )

    job_id = harmony_client.submit(request)
    harmony_client.wait_for_processing(job_id, show_progress=True)
    results = harmony_client.download_all(job_id)
    file_path = results[0].result()

    datatree = xr.open_datatree(file_path)
    data_array = datatree["product/vertical_column_troposphere"]
    quality_flags = datatree["product/main_data_quality_flag"]
    good_array = data_array.where(quality_flags == 0).squeeze()

    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    lats = datatree["geolocation/latitude"].values
    lons = datatree["geolocation/longitude"].values

    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            value = float(good_array.values[i, j])
            timestamp = dt.datetime.utcnow()
            cursor.execute(
                "INSERT INTO tempo_no2 (timestamp, lon, lat, value) VALUES (%s, %s, %s, %s)",
                (timestamp, float(lon), float(lat), value)
            )

    conn.commit()
    conn.close()
    print(f"✓ Data stored in MySQL at {dt.datetime.utcnow()}")
