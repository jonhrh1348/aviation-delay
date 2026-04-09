import csv
import json
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient, TopicPartition

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
def insert_to_clickhouse(consumer_instance, table_name, client, column_names, defaults):
    print(f"Starting consumption of data from Kafka with client_id: {consumer_instance.config['client_id']}")
    batch= []

    for message in consumer_instance:
      row_item = message.value

      match table_name:
        case 'raw_aviation_flights':
          row = [row_item.get(col, defaults[col]) for col in column_names[:6]]
          cs_item = row_item.get('codeshared', {})
          row.append(cs_item.get('airline', {}))
          row.append(cs_item.get('flight', {}))

        case 'historical_weather_data':
          weather_time = row_item.get('dt', 0)
          row = [row_item.get(col, defaults[col]) for col in column_names[1:]]
          row.insert(0, weather_time)

        case _:
          print (f'{table_name} is not found in database')
          continue

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

def consume_kafka_messages(consumer_instance):
    consumed = []
    for message in consumer_instance:
        consumed.append(message.value)
    return consumed
