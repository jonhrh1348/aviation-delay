import csv
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition
from src.utils.helper import safe_get

# Kafka Producer
def create_kafka_producer(client_id, bootstrap_servers):
  return KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    client_id=client_id
  )

# Kafka Consumer
def create_kafka_consumer(client_id, bootstrap_servers, topic_name, group_id=None):
  return KafkaConsumer(
      topic_name,
      bootstrap_servers=bootstrap_servers,
      client_id=client_id,
      group_id=group_id,
      value_deserializer=lambda x: json.loads(x.decode('utf-8')),
      consumer_timeout_ms=10000
  )

# Streaming function
def stream_to_kafka(data_csv, producer, topic_name): 
    print(f"Starting data streaming to Kafka with producer client_id: {producer.config['client_id']}")
    
    record_count = 0
    # Open the CSV file in read mode
    with open(data_csv, mode='r', encoding='utf-8') as file:
        csv_reader = csv.DictReader(file)
        
        for record in csv_reader:
            producer.send(topic_name, record)
            record_count += 1
            
    producer.flush()
    print(f"Sent {record_count} records from CSV to Kafka topic: {topic_name}")

# Inject into clickhouse after kafka producer reads it
def insert_to_clickhouse(consumer_instance, table_name, client, column_names=None, defaults=None):
    print(f"Starting consumption of data from Kafka with client_id: {consumer_instance.config['client_id']}")
    batch= []

    for message in consumer_instance:
      row = process_rows(table_name, message.value, column_names, defaults)
      if row:
        batch.append(row)

      # Batch insert every 500 records
      if len(batch) >= 500:
        client.insert(table_name, batch, column_names=column_names)
        print(f"Successfully inserted {len(batch)} records to ClickHouse.")
        batch = []

    # Final insert for remaining records
    if batch:
        client.insert(table_name, batch, column_names=column_names)
        print(f"Successfully inserted {len(batch)} remaining records to ClickHouse.")

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
        row = {
          "flight_type": safe_get(row_item, "type", default_value="Nil"),
          "status": safe_get(row_item, "status", default_value="Unknown"),
          "iata_number": safe_get(row_item, "flight", "iataNumber", default_value=""),
          "airline_name": safe_get(row_item, "airline", "name", default_value=""),
          "arr_scheduled_time": safe_get(row_item, "arrival", "scheduledTime", default_value="2099-12-31t00:00:00.000"),
          "arr_actual_time": safe_get(row_item, "arrival", "actualTime", default_value="2099-12-31t00:00:00.000"),
          "arr_delay_mins": safe_get(row_item, "arrival", "delay", default_value=0),
          "dep_scheduled_time": safe_get(row_item, "departure", "scheduledTime", default_value="2099-12-31t00:00:00.000"),
          "dep_actual_time": safe_get(row_item, "departure", "actualTime", default_value="2099-12-31t00:00:00.000"),
          "dep_delay_mins": safe_get(row_item, "departure", "delay", default_value=0),
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