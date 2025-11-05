import os
import time
import json
import logging
from dotenv import load_dotenv
from kafka.errors import KafkaError

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "weather_producer.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def build_weather():
    from ingest.weather import fetch_weather

    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'env', '.env'))
    city = os.getenv('CITY_NAME', 'Dubai')

    weather_data = fetch_weather()
    if not weather_data:
        logging.error("🚨 No weather data fetched, skipping batch")
        return None, None

    value = weather_data.copy()
    value["city"] = city
    value["event_timestamp"] = int(time.time())
    key = city 

    return key, value

def send_weather():
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'env', '.env'))
    topic = os.getenv('KAFKA_WEATHER', 'weather')

    from producers.producer_info import create_producer
    producer = create_producer()

    key, value = build_weather()
    if not key:
        logging.warning("⚠️ Weather record invalid — not sent to Kafka")
        return None, None

    success, failed = 0, 0

    try:
        producer.send(topic, key=key, value=value).get(timeout=10)
        success += 1
    except KafkaError as e:
        failed += 1
        logging.error(f"🚨 Failed to send weather record for {key}: {e}")

    producer.flush()
    producer.close()

    logging.info(f"🌦️ Weather batch — ✅ {success} | ❌ {failed}")
    return value, success, failed

def send_weather_streaming():
    print("🌦️ Starting live weather streaming... (CTRL+C to stop)")
    try:
        while True:
            data, success, failed = send_weather()

            if data:
                current_time = time.strftime("%Y-%m-%d %H:%M:%S")
                print(
                    f"📡 {current_time} — Weather sent | "
                    f"✅ {success} | ❌ {failed} | "
                    f"🌡️ {data.get('temperature')}°C | "
                    f"☁️ {data.get('cloudiness')}% | "
                    f"💨 {data.get('wind_speed')} m/s | "
                    f"🌧️ {data.get('precipitation')} mm"
                )

            time.sleep(1800) # 30 min

    except KeyboardInterrupt:
        print("\n🛑 Weather streaming stopped by user.")
    except Exception as e:
        logging.critical(f"🔥 Fatal error in weather streaming: {e}")

if __name__ == "__main__":
    send_weather_streaming()