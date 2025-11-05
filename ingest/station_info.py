import os
import json
import logging
from dotenv import load_dotenv
from utils.http_client import safe_fetch_json as fetch_json

logging.basicConfig(
    filename="logs/station_info.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "env", ".env"))

GBFS_ROOT = os.getenv("GBFS_ROOT")
LANG = os.getenv("GBFS_LANGUAGE", "en")

if not GBFS_ROOT:
    raise RuntimeError("❌ GBFS_ROOT not set in env/.env file!")
if not GBFS_ROOT.startswith("http"):
    raise ValueError("❌ Invalid GBFS_ROOT URL!")

def station_info_url():
    gbfs_index = fetch_json(GBFS_ROOT)
    if not gbfs_index:
        logging.error("🚨 GBFS Index unavailable — cannot fetch stations")
        return None

    feeds = gbfs_index.get("data", {}).get(LANG, {}).get("feeds", [])

    for feed in feeds:
        if feed.get("name") == "station_information":
            return feed.get("url")

    logging.error("❌ station_information feed not found in GBFS index")
    return None

def fetch_station_information():
    url = station_info_url()
    if not url:
        return []

    data = fetch_json(url)
    if not data:
        logging.error("🚨 Failed fetching station_information data")
        return []

    stations = data.get("data", {}).get("stations", [])
    info = []

    for station in stations:

        if not station.get("station_id") or station.get("capacity") is None:
            logging.warning(f"⚠️ Skipped invalid station: {station}")
            continue

        info.append({
            "station_id": station.get("station_id"),
            "name": station.get("name", "Unknown"),
            "lat": station.get("lat"),
            "lon": station.get("lon"),
            "capacity": station.get("capacity"),
        })

    logging.info(f"✅ Loaded {len(info)} valid stations")
    return info

if __name__ == "__main__":
    stations = fetch_station_information()
    print(json.dumps({
        "count": len(stations),
        "sample": stations[:5]
    }, indent=2))