from datetime import datetime, timedelta

def safe_get(obj, *path, default_value=None):
    for key in path:
        if isinstance(obj, dict) and key in obj:
            obj = obj.get(key)
        else:
            return default_value
    return obj

def get_actual_time(row_item, time_type):
    """Calculate actual time by adding delay mins to scheduled time if actual time is missing."""
    actual = safe_get(row_item, time_type, "actualTime")
    if actual:
        return actual
    
    scheduled = safe_get(row_item, time_type, "scheduledTime")
    delay = safe_get(row_item, time_type, "delay", default_value=0)
    
    if scheduled:
        dt = datetime.fromisoformat(scheduled)
        actual_dt = dt + timedelta(minutes=delay)
        return actual_dt.isoformat()
    
    return "2099-12-31t00:00:00.000"

def process_rows(table_name, row_item, column_names, defaults):
    match table_name:
      case 'raw_aviation_flights':
        row = [row_item.get(col, defaults[col]) for col in column_names[:6]]
        cs_item = row_item.get('codeshared', {})
        row.append(cs_item.get('airline', {}))
        row.append(cs_item.get('flight', {}))

      case 'raw_hist_weather_data':
        weather_time = row_item.get('dt', 0)
        row = [row_item.get(col, defaults[col]) for col in column_names[1:]]
        row.insert(0, weather_time)

      case 'aviation_flights':
        row= {
          "flight_type": safe_get(row_item, "type", default_value="Nil"),
          "status": safe_get(row_item, "status", default_value="Unknown"),
          "iata_number": safe_get(row_item, "flight", "iataNumber", default_value=""),
          "airline_name": safe_get(row_item, "airline", "name", default_value=""),
          "dep_scheduled_time": safe_get(row_item, "departure", "scheduledTime", default_value="2099-12-31t00:00:00.000"), 
          "dep_actual_time": get_actual_time(row_item, "departure"),
          "dep_delay_mins": safe_get(row_item, "departure", "delay", default_value=0),
          "arr_scheduled_time": safe_get(row_item, "arrival", "scheduledTime", default_value="2099-12-31t00:00:00.000"),
          "arr_actual_time": get_actual_time(row_item, "arrival"),
          "arr_delay_mins": safe_get(row_item, "arrival", "delay", default_value=0),
          "codeshared_airline": safe_get(row_item, "codeshared", "airline", "name", default_value=""),
          "codeshared_flight_number": safe_get(row_item, "codeshared", "flight", "iataNumber", default_value=""),
        }
      
      case 'historical_weather_data':
        row = {
          "date_observed": safe_get(row_item, "dt", default_value="2099-12-31t00:00:00.000"),
          "current_temp": safe_get(row_item, "main", "temp", default_value=999.99),
          "feels_like_temp": safe_get(row_item, "main", "feels_like", default_value=999.99),
          "pressure_hPa": safe_get(row_item, "main", "pressure", default_value=-100),
          "humidity_pct": safe_get(row_item, "main", "humidity", default_value=-100),
          "min_temp": safe_get(row_item, "main", "temp_min", default_value=-999.99),
          "max_temp": safe_get(row_item, "main", "temp_max", default_value=999.99),
          "wind_speed_ms": safe_get(row_item, "wind", "speed", default_value=0.00),
          "wind_deg": safe_get(row_item, "wind", "deg", default_value=0),
          "cloudiness_pct": safe_get(row_item, "clouds", "all", default_value=-100),
          "rain_1h": safe_get(row_item, "rain", "1h", default_value=0.00),
          "rain_3h": safe_get(row_item, "rain", "3h", default_value=0.00),
          "weather_main": [w.get("main") for w in row_item.get("weather", []) if w.get("main")] if row_item.get("weather") else [],
          "weather_desc": [w.get("description") for w in row_item.get("weather", []) if w.get("description")] if row_item.get("weather") else [],
      }

      case _:
        print (f'{table_name} is not found in database')