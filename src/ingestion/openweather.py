import datetime
import requests

def fetch_weather_history(api_key, lat, lon, start_date, end_date, interval_days=7):
    all_records = []
    url = "https://history.openweathermap.org/data/2.5/history/city"
    interval = datetime.timedelta(days=interval_days)

    current = start_date
    while current < end_date:
        next_date = min(current + interval, end_date)
        print(f"Starting data collection from {current.date()} to {next_date.date()}.")

        params = {
            "lat": lat,
            "lon": lon,
            "type": "hour",
            "start": int(current.timestamp()),
            "end": int(next_date.timestamp()),
            "appid": api_key,
            "units": "metric"
        }

        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()

            if "list" in data:
                all_records.extend(data["list"])
        except Exception as e:
            print(f'Error fetching data from historical weather API: {e}')

        current = next_date

    return all_records

def fetch_forecasted_weather_data(api_key, lat, lon): 
    url = 'https://pro.openweathermap.org/data/2.5/forecast/hourly'
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        all_records = data['list']
    except Exception as e:
        print(f'Error fetching data from current hour weather API: {e}')
    
    return all_records

def current_hour_weather_data(api_key, lat, lon):
    url = 'https://api.openweathermap.org/data/2.5/weather'
    params = {
        "lat": lat,
        "lon": lon,
        "appid": api_key,
        "units": "metric"
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f'Error fetching data from current hour weather API: {e}')
    
    return data