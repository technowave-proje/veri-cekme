import requests
import mysql.connector
from datetime import datetime
import time

API_KEY = "7304163140f3757aba23c36eb573d9ec"

cities = [
    {"city": "New York", "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"city": "Los Angeles", "country": "US", "lat": 34.0522, "lon": -118.2437},
    {"city": "Toronto", "country": "CA", "lat": 43.6532, "lon": -79.3832},
    {"city": "Mexico City", "country": "MX", "lat": 19.4326, "lon": -99.1332},
    # diğer şehirleri ekle...
]

# MySQL bağlantısı
db = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Ankara10.",
    database="weather_db"
)
cursor = db.cursor()

for city in cities:
    # Güncel Hava Durumu API çağrısı
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={city['lat']}&lon={city['lon']}&appid={API_KEY}&units=metric&lang=tr"
    response = requests.get(url).json()
    
    temp = response['main']['temp']
    feels_like = response['main']['feels_like']
    humidity = response['main']['humidity']
    pressure = response['main']['pressure']
    weather_main = response['weather'][0]['main']
    weather_desc = response['weather'][0]['description']
    wind_speed = response['wind']['speed']
    cloudiness = response['clouds']['all']
    rain_1h = response.get('rain', {}).get('1h', 0.0)
    
    last_updated = datetime.utcfromtimestamp(response['dt'])
    
    # Veritabanına ekle / güncelle
    cursor.execute("""
        INSERT INTO city_weather
        (city, country, lat, lon, temp, feels_like, humidity, pressure, weather_main, weather_desc, wind_speed, cloudiness, rain_1h, last_updated)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE 
            temp=%s, feels_like=%s, humidity=%s, pressure=%s, weather_main=%s, weather_desc=%s, wind_speed=%s, cloudiness=%s, rain_1h=%s, last_updated=%s
    """, (
        city['city'], city['country'], city['lat'], city['lon'], temp, feels_like, humidity, pressure,
        weather_main, weather_desc, wind_speed, cloudiness, rain_1h, last_updated,
        temp, feels_like, humidity, pressure, weather_main, weather_desc, wind_speed, cloudiness, rain_1h, last_updated
    ))
    
    db.commit()
    
    # Rate limit için bekleme
    time.sleep(1)

cursor.close()
db.close()
