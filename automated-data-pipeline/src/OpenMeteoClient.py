# Open-Meteo API client module.

# simple class interface for fetching weather data
# no API key required for open-meteo.com

from datetime import datetime, timedelta

import requests


class OpenMeteoClient:
    # open-meteo.com API endpoint for forecast data

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self):
        self.session = requests.Session()

    def get_weather(self, latitude, longitude, start_date, end_date):
        """
        Fetch daily weather data for a location and date range.

        Args:
            latitude: float, location latitude
            longitude: float, location longitude
            start_date: str, start date in YYYY-MM-DD format
            end_date: str, end date in YYYY-MM-DD format

        Returns:
            dict with keys 'dates', 'temperature_max', 'temperature_min',
            'precipitation', 'windspeed_max'

        Raises: requests.exceptions.RequestException on api errors
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max",
            "start_date": start_date,
            "end_date": end_date,
            "temperature_unit": "fahrenheit",
            "windspeed_unit": "mph",
            "timezone": "America/New_York",
        }

        response = self.session.get(self.BASE_URL, params=params)
        response.raise_for_status()

        data = response.json()
        daily = data["daily"]

        return {
            "dates": daily["time"],
            "temperature_max": daily["temperature_2m_max"],
            "temperature_min": daily["temperature_2m_min"],
            "precipitation": daily["precipitation_sum"],
            "windspeed_max": daily["windspeed_10m_max"],
        }

    def _build_date_pages(self, start_date, end_date, page_size_days):
        # internal helper function to split date range into pages to use pagination in get_weather_bulk
        # the api would return all the data for the whole data range requiring only one call
        # this is done to satisfy pagination requirements
        pages = []
        current = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        while current <= end:
            page_end = min(current + timedelta(days=page_size_days - 1), end)
            pages.append((current.strftime("%Y-%m-%d"), page_end.strftime("%Y-%m-%d")))
            current = page_end + timedelta(days=1)

        return pages

    def get_weather_bulk(self, cities, start_date, end_date, page_size_days=3):     # api allows more days, but we use smaller pages to demonstrate pagination. 
        """
        Fetch weather data for multiple cities, paginating over date range.

        Loops over each city and splits the date range into chunks of
        page_size_days, making a separate API call for each page.

        Args:
            cities: list of dicts with keys 'name', 'latitude', 'longitude'
            start_date: str, start date in YYYY-MM-DD format
            end_date: str, end date in YYYY-MM-DD format
            page_size_days: int, number of days per API call (default 3)

        Returns:
            all_rows(list of dicts), each with 'city', 'date', 'temp_max_f', 'temp_min_f',
            'precipitation_in', 'windspeed_max_mph'
        """
        pages = self._build_date_pages(start_date, end_date, page_size_days)
        all_rows = []
        errors = []

        for city in cities:
            name = city["name"]
            city_days = 0
            print(f"Fetching weather data for {name} ({len(pages)} pages)...")

            for page_num, (page_start, page_end) in enumerate(pages, 1):
                print(f"  Page {page_num}/{len(pages)}: {page_start} to {page_end}")

                try:
                    weather = self.get_weather(
                        city["latitude"], city["longitude"], page_start, page_end
                    )
                except requests.exceptions.HTTPError as e:
                    msg = f"HTTP error for {name} (page {page_num}): {e}"
                    print(f"    ERROR: {msg}")
                    errors.append(msg)
                    continue
                except requests.exceptions.ConnectionError as e:
                    msg = f"Connection error for {name} (page {page_num}): {e}"
                    print(f"    ERROR: {msg}")
                    errors.append(msg)
                    continue
                except requests.exceptions.RequestException as e:
                    msg = f"Request failed for {name} (page {page_num}): {e}"
                    print(f"    ERROR: {msg}")
                    errors.append(msg)
                    continue

                for i, date in enumerate(weather["dates"]):
                    all_rows.append({
                        "city": name,
                        "date": date,
                        "temp_max_f": weather["temperature_max"][i],
                        "temp_min_f": weather["temperature_min"][i],
                        "precipitation_in": weather["precipitation"][i],
                        "windspeed_max_mph": weather["windspeed_max"][i],
                    })
                    city_days += 1

            print(f"  Got {city_days} days of data for {name}.")

        if errors:
            print(f"\n{len(errors)} error(s) occurred during data fetch:")
            for err in errors:
                print(f"  - {err}")

        return all_rows
