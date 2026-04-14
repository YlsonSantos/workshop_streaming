import csv
import io
import json
import time
from datetime import datetime, timezone
import requests
from kafka import KafkaProducer

DATASET_URL = "https://raw.githubusercontent.com/jpatokal/openflights/master/data/airports.dat"
TOPIC_NAME = "airports_raw"
BOOTSTRAP_SERVERS = "localhost:9092"
CHUNK_SIZE = 25
CHUNK_INTERVAL_SECONDS = 5
RECORD_INTERVAL_SECONDS = 0.15

COLUMNS = [
    "airport_id", "name", "city", "country", "iata", "icao",
    "latitude", "longitude", "altitude", "timezone", "dst",
    "tz_database_timezone", "type", "source"
]

def normalize_value(value):
    return None if value == r"\N" else value

def chunked(iterable, size):
    chunk = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) == size:
            yield chunk
            chunk = []
    if chunk: yield chunk

def load_dataset_rows():
    response = requests.get(DATASET_URL, timeout=60)
    response.raise_for_status()
    reader = csv.reader(io.StringIO(response.text))
    for row in reader:
        if len(row) != len(COLUMNS): continue
        payload = {col: normalize_value(val) for col, val in zip(COLUMNS, row)}
        payload["event_time"] = datetime.now(timezone.utc).isoformat()
        yield payload

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8")
    )
    total_sent = 0
    for chunk_num, records in enumerate(chunked(load_dataset_rows(), CHUNK_SIZE), start=1):
        print(f"\nEnviando chunk {chunk_num}...")
        for i, record in enumerate(records, start=1):
            record["chunk_number"] = chunk_num
            record["chunk_position"] = i
            producer.send(TOPIC_NAME, key=record["airport_id"], value=record)
            total_sent += 1
            time.sleep(RECORD_INTERVAL_SECONDS)
        producer.flush()
        print(f"Total acumulado: {total_sent}")
        time.sleep(CHUNK_INTERVAL_SECONDS)
    producer.close()

if __name__ == "__main__":
    main()