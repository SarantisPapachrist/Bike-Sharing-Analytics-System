import time
import logging
import requests

logging.basicConfig(
    filename="logs/api_errors.log",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def safe_fetch_json(url, retries=5, timeout=10, backoff_factor=1.5):
    """Fetch JSON from API with retry & backoff"""

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()

            if not isinstance(data, dict):
                raise ValueError("❌ API returned non-JSON object")

            return data
        
        except Exception as e:
            logging.warning(
                f"Attempt {attempt}/{retries} - URL: {url} - Error: {e}"
            )
            wait = backoff_factor ** attempt
            time.sleep(wait)

    logging.error(f"🚨 FINAL FAILURE - URL unreachable: {url}")
    return None