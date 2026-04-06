import datetime
import requests

def fetch_flight_history(api_key, airport_code, flight_types, start_date, end_date, interval_days=7):
    all_records = []
    url = "https://aviation-edge.com/v2/public/flightsHistory"
    interval = datetime.timedelta(days=interval_days)

    for flight_type in flight_types:
        current = start_date
        while current < end_date:
            next_date = min(current + interval, end_date)

            params = {
                "key": api_key,
                "code": airport_code,
                "type": flight_type,
                "date_from": current.strftime("%Y-%m-%d"),
                "date_to": next_date.strftime("%Y-%m-%d"),
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if isinstance(data, list):
                all_records.extend(data)

            current = next_date

    return all_records