"""
Automated weather data pipeline.

Fetches weather data for Florida panhandle cities using the Open-Meteo API,
saves raw JSON and delegates all persistent storage to storage.py.
"""

import json
import logging
import os
from datetime import date, datetime, timedelta

from OpenMeteoClient import OpenMeteoClient
import storage

#setup logging config to handle logging throughout, gets time and messages
#format is timestamp, log type, and logging messages
logging.basicConfig(
    filename=os.path.join("..", "logs", "pipeline.log"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


# Florida panhandle cities
CITIES = [
    {"name": "Tallahassee", "latitude": 30.4383, "longitude": -84.2807},
    {"name": "Pensacola",   "latitude": 30.4213, "longitude": -87.2169},
    {"name": "Navarre",     "latitude": 30.4016, "longitude": -86.8633},
]

# Date range: last 14 days (gives 42 rows across 3 cities)
END_DATE   = date.today().isoformat()
START_DATE = (date.today() - timedelta(days=13)).isoformat()

# Raw JSON dump path (temporary — see TODO below)
RAW_JSON_PATH = os.path.join("data", "raw", "weather_raw.json")


# TODO this is just a temp json dump to help view the raw data stucture.
# to be deleted in the future. 

# Jack - Not deleting because im scared it'll break something

def save_raw_json(rows: list[dict], path: str) -> None:
    """raw data -> JSON"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(rows, f, indent=2)
    logging.info(f"Saved raw JSON to {path}")
    print(f"Saved raw JSON to {path}")

# Helper functions

def fetch_weather(cities: list[dict], start: str, end: str) -> list[dict]:
    """
    Call the Open-Meteo API for all cities and return collected rows
    """
    client = OpenMeteoClient()
    rows = client.get_weather_bulk(cities, start, end)
    return rows


def stamp_rows(rows: list[dict]) -> list[dict]:
    """Add a run_timestamp to each row (in-place) and return the list."""
    ts = datetime.now().isoformat()
    for row in rows:
        row["run_timestamp"] = ts
    return rows

def main() -> None:
    logging.info("Pipeline started")
    print("=" * 50)
    print("Weather Data Pipeline")
    print(f"Date range : {START_DATE} to {END_DATE}")
    print(f"Cities     : {', '.join(c['name'] for c in CITIES)}")
    print("=" * 50)

    # Fetch data from API
    try:
        rows = fetch_weather(CITIES, START_DATE, END_DATE)
        logging.info("Weather data fetched successfully")
    except Exception as e:
        logging.error(f"Pipeline failed during fetch: {e}")
        print(f"\nPIPELINE ERROR: unexpected failure — {e}")
        return

    if not rows:
        logging.error("No data collected — aborting pipeline")
        print("\nPIPELINE ERROR: no data was collected.")
        return

    # Stamp each row with the current run timestamp
    rows = stamp_rows(rows)

    # Save raw JSON
    save_raw_json(rows, RAW_JSON_PATH)

    # Persist to CSV
    storage.save_csv(rows)
    logging.info(f"CSV saved to {storage.CSV_PATH}")

    # Persist to SQLite
    storage.save_sqlite(rows)
    logging.info(f"SQLite saved to {storage.DB_PATH}")

    print(f"\nSUCCESS: collected and stored {len(rows)} records.")
    logging.info(f"Pipeline completed successfully with {len(rows)} total records")


if __name__ == "__main__":
    main()
