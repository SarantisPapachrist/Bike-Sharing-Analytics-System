import os
import json
import logging
from dotenv import load_dotenv
from utils.http_client import safe_fetch_json as fetch_json

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
logging.basicConfig(
    filename=os.path.join(log_dir, "station_status.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "env", ".env"))
GBFS_ROOT = os.getenv("GBFS_ROOT")
LANG = os.getenv("GBFS_LANGUAGE", "en")

if not GBFS_ROOT or not GBFS_ROOT.startswith("http"):
    raise ValueError("❌ Invalid or missing GBFS_ROOT in .env")

def station_status_url():
    gbfs_index = fetch_json(GBFS_ROOT)
    if not gbfs_index:
        logging.error("🚨 GBFS index unavailable!")
        return None

    feeds = gbfs_index.get("data", {}).get(LANG, {}).get("feeds", [])
    for feed in feeds:
        if feed.get("name") == "station_status":
            return feed.get("url")

    logging.error("❌ station_status feed not found in GBFS index")
    return None


def fetch_station_status():
    url = station_status_url()
    if not url:
        logging.error("❌ No station_status URL found!")
        return []

    data = fetch_json(url)
    if not data:
        logging.error("🚨 Failed to fetch station_status data")
        return []

    stations = data.get("data", {}).get("stations", [])
    status = []

    for station in stations:
        sid = station.get("station_id")
        bikes = station.get("num_bikes_available")
        docks = station.get("num_docks_available")

        if not sid:
            logging.warning(f"⚠️ Skipped invalid station status: {station}")
            continue

        status.append({
            "station_id": sid,
            "num_bikes_available": bikes if bikes is not None else 0,
            "num_docks_available": docks if docks is not None else 0,
            "is_installed": station.get("is_installed", True),
            "is_renting": station.get("is_renting", True),
            "is_returning": station.get("is_returning", True),
            "last_reported": station.get("last_reported"),
        })

    logging.info(f"✅ Loaded {len(status)} valid status entries")
    return status

if __name__ == "__main__":
    st = fetch_station_status()
    print(json.dumps({"count": len(st), "sample": st[:3]}, indent=2))
