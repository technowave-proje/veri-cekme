import requests
import mysql.connector
from datetime import datetime
import time

API_KEY = "7304163140f3757aba23c36eb573d9ec"

# Örnek şehir listesi (tüm Kuzey Amerika şehirleri için genişletebilirsin)
cities = [
    {"city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"city": "Los Angeles", "country": "US", "lat": 34.0522, "lon": -118.2437},
    {"city": "Toronto", "country": "CA", "lat": 43.6532, "lon": -79.3832},
    {"city": "Mexico City", "country": "MX", "lat": 19.4326, "lon": -99.1332},
    # diğer şehirleri ekle...
]

# Veritabanı bağlantısı 
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Ankara10.",
    database="weather_db"
)
cursor = db.cursor()

for city in cities:
    url = f"http://api.openweathermap.org/data/2.5/air_pollution?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}"
    response = requests.get(url).json()
    
    aqi = response['list'][0]['main']['aqi']
    components = response['list'][0]['components']
    now = datetime.now()    
    
    cursor.execute("""
        INSERT INTO air_quality (city, country, lat, lon, aqi, pm25, pm10, o3, no2, co, so2, last_updated)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE aqi=%s, pm25=%s, pm10=%s, o3=%s, no2=%s, co=%s, so2=%s, last_updated=%s
    """, (
        city['city'], city['country'], city['lat'], city['lon'], aqi,
        components['pm2_5'], components['pm10'], components['o3'], components['no2'],
        components['co'], components['so2'], now,
        aqi, components['pm2_5'], components['pm10'], components['o3'], components['no2'],
        components['co'], components['so2'], now
    ))
    
    # API limitini aşmamak için kısa süre bekle
    time.sleep(1)  # her şehirden sonra 1 saniye bekle

db.commit()
cursor.close()
db.close()
