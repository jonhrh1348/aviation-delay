import datetime
import requests

def fetch_weather_history(api_key, lat, lon, start_date, end_date, interval_days=7):
    all_records = []
    url = "https://history.openweathermap.org/data/2.5/history/city"
    interval = datetime.timedelta(days=interval_days)

    current = start_date
    while current < end_date:
        next_date = min(current + interval, end_date)

        params = {
            "lat": lat,
            "lon": lon,
            "type": "hour",
            "start": int(current.timestamp()),
            "end": int(next_date.timestamp()),
            "appid": api_key,
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "list" in data:
            all_records.extend(data["list"])

        current = next_date

    return all_records