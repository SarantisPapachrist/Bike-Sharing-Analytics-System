import os
import json
import logging
from dotenv import load_dotenv
from kafka import KafkaProducer

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "producer_info.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def create_producer():
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'env', '.env'))
    broker = os.getenv('KAFKA_BROKER')
    
    if not broker:
        raise ValueError("❌ KAFKA_BROKER not set in .env")

    try:
        producer = KafkaProducer(
            bootstrap_servers=[broker],
            acks='all',
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            linger_ms=20,
            retries=5,
            max_in_flight_requests_per_connection=1
        )
        logging.info(f"✅ Kafka Producer connected to {broker}")
        return producer
    except Exception as e:
        logging.error(f"🚨 Failed to create Kafka producer: {e}")
        raise

def build_info():
    from ingest.station_info import fetch_station_information
    stations = fetch_station_information()
    info = []

    for station in stations:
        sid = station.get("station_id")

        if not sid:
            logging.warning(f"⚠️ Skipped station without station_id: {station}")
            continue

        info.append((sid, station))

    return info

def send_info():
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', 'env', '.env'))
    topic = os.getenv('KAFKA_STATION_INFO', "station_info")

    producer = create_producer()
    info = build_info()

    success = 0
    failed = 0

    for key, value in info:
        try:
            producer.send(topic, key=key, value=value).get(timeout=10)
            success += 1
        except Exception as e:
            failed += 1
            logging.error(f"🚨 Failed to send record (key={key}) to {topic} → {e}")

    producer.flush()
    producer.close()

    logging.info(f"📡 Sent {success} messages ✅ | ❌ Failed: {failed}")
    return success

if __name__ == "__main__":
    count = send_info()
    print(f"✅ Sent {count} station_info messages to Kafka")