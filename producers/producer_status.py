import os
import time
import json
import logging
from dotenv import load_dotenv
from kafka.errors import KafkaError

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "producer_status.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def build_status():
    from ingest.station_status import fetch_station_status

    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'env', '.env'))
    city = os.getenv('CITY_NAME', 'Dubai')

    station_status = fetch_station_status()
    status = []

    for station in station_status:
        sid = station.get("station_id")
        if not sid:
            logging.warning(f"⚠️ Skipped record with no station_id: {station}")
            continue

        record = station.copy()
        record["city"] = city
        record["event_timestamp"] = int(time.time())

        status.append((str(sid), record))

    return status

def send_status():
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'env', '.env'))
    topic = os.getenv('KAFKA_STATION_STATUS', 'station_status')

    from producers.producer_info import create_producer
    producer = create_producer()

    messages = build_status()

    success = 0
    failed = 0

    for key, value in messages:
        try:
            producer.send(topic, key=key, value=value).get(timeout=10)
            success += 1
        except KafkaError as e:
            failed += 1
            logging.error(f"🚨 Kafka send failed for key={key}: {e}")

    producer.flush()
    producer.close()

    logging.info(f"📡 Batch sent: ✅ {success} | ❌ {failed}")

    return success, failed


def send_status_streaming():
    print("🚴 Starting live station status streaming... (CTRL+C to stop)")
    try:
        while True:
            success, failed = send_status()
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")

            print(
                f"📡 {current_time} — Sent ✅{success} | ❌Failed {failed} "
                f"| Stations: {success + failed}"
            )

            time.sleep(300)

    except KeyboardInterrupt:
        print("\n🛑 Stopped live station status streaming.")
    except Exception as e:
        logging.critical(f"🔥 Fatal error in streaming loop: {e}")

if __name__ == "__main__":
    send_status_streaming()