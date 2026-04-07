"""
Automated weather data pipeline.

Fetches weather data for Florida panhandle cities using the Open-Meteo API,
saves raw JSON and processed CSV output.
"""

import json
import os
from datetime import date, timedelta

from OpenMeteoClient import OpenMeteoClient


# Florida panhandle cities that William outlined in README
# you can't call open meteo with city names, so I found the lat and long for each city.
CITIES = [
    {"name": "Tallahassee", "latitude": 30.4383, "longitude": -84.2807},
    {"name": "Pensacola",   "latitude": 30.4213, "longitude": -87.2169},
    {"name": "Navarre",     "latitude": 30.4016, "longitude": -86.8633},
]

# date range: last 14 days (gives 42+ rows across 3 cities)
END_DATE = date.today().isoformat()
START_DATE = (date.today() - timedelta(days=13)).isoformat()

# output paths
RAW_JSON_PATH = os.path.join("data", "raw", "weather_raw.json")


# TODO this is just a temp json dump to help view the raw data stucture.
# to be deleted in the future. 
def save_json(rows, path):
    """raw data -> JSON"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"Saved raw JSON to {path}")

def main():
    print("=" * 50)
    print("Weather Data Pipeline")
    print(f"Date range: {START_DATE} to {END_DATE}")
    print(f"Cities: {', '.join(c['name'] for c in CITIES)}")
    print("=" * 50)

    client = OpenMeteoClient()

    try:
        rows = client.get_weather_bulk(CITIES, START_DATE, END_DATE)
    except Exception as e:
        print(f"\nPIPELINE ERROR: unexpected failure - {e}")
        return

    if not rows:
        print("\nPIPELINE ERROR: no data was collected.")
        return

    # TODO append raw json rows to csv/sqllite db
    # structure of raw rows found in raw/weather_raw.json
    # temp structure of processed rows (to be saved in csv/sqlite instead of json):
    save_json(rows, RAW_JSON_PATH)
    

    print(f"\nSUCCESS: collected {len(rows)} total records.")


if __name__ == "__main__":
    main()
