import os
import json
import time
import logging
from dotenv import load_dotenv
from utils.http_client import safe_fetch_json as fetch_json

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "weather.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "env", ".env"))

API_KEY = os.getenv("WEATHER_API_KEY")
LAT = os.getenv("CITY_LAT")
LON = os.getenv("CITY_LON")

if not API_KEY:
    raise RuntimeError("❌ Missing WEATHER_API_KEY in .env!")

def fetch_weather():
    url = (
        "https://api.openweathermap.org/data/2.5/weather"
        f"?lat={LAT}&lon={LON}&appid={API_KEY}&units=metric"
    )

    data = fetch_json(url)
    if not data:
        logging.error("🚨 Weather API failed, using fallback values")
        return {
            "timestamp": int(time.time()),
            "temperature": None,
            "wind_speed": None,
            "cloudiness": None,
            "precipitation": None
        }

    main = data.get("main", {})
    wind = data.get("wind", {})
    clouds = data.get("clouds", {})
    rain = data.get("rain", {})

    weather = {
        "timestamp": data.get("dt", int(time.time())),
        "temperature": main.get("temp"),
        "wind_speed": wind.get("speed"),
        "cloudiness": clouds.get("all"),
        "precipitation": rain.get("1h", 0.0)
    }

    logging.info(f"🌦️ Weather fetched: {weather}")
    return weather

if __name__ == "__main__":
    print(json.dumps(fetch_weather(), indent=2))