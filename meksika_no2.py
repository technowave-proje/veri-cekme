from netCDF4 import Dataset
import numpy as np
import mysql.connector

# NetCDF dosyasını aç
dataset = Dataset("110122552_TEMPO_NO2_L3_V03_20250703T200954Z_S012_subsetted.nc4", "r")

# Koordinatları al
lat = dataset.variables['latitude'][:]
lon = dataset.variables['longitude'][:]

# NO2 verisi (weight değişkeninde)
no2_data = dataset.variables['weight'][:, :]

# Meksika sınırları
lat_min, lat_max = 14, 33
lon_min, lon_max = -118, -86

# Meksika’daki noktaların indekslerini bul
lat_idx = np.where((lat >= lat_min) & (lat <= lat_max))[0]
lon_idx = np.where((lon >= lon_min) & (lon <= lon_max))[0]

# Sadece Meksika’daki verileri al
no2_mexico = no2_data[lat_idx.min():lat_idx.max()+1, lon_idx.min():lon_idx.max()+1]

# MySQL bağlantısı
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Ankara10.",
    database="tempoo_data"
)
cursor = conn.cursor()

# Meksika’daki grid noktalarını ve değerlerini tabloya ekle
for i, lat_i in enumerate(lat[lat_idx.min():lat_idx.max()+1]):
    for j, lon_j in enumerate(lon[lon_idx.min():lon_idx.max()+1]):
        value = no2_mexico[i, j]
        if value is not np.ma.masked:  # Sadece gerçek değerler
            cursor.execute(
                "INSERT INTO tempoo_data (latitude, longitude, no2_value) VALUES (%s, %s, %s)",
                (float(lat_i), float(lon_j), float(value))
            )

conn.commit()
cursor.close()
conn.close()
