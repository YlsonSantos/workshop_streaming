import json
import os
import time
from datetime import datetime
import pandas as pd
from kafka import KafkaConsumer

TOPIC_NAME = "airports_raw"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "bronze-consumer-group"
BRONZE_PATH = "data/bronze/airports"
BATCH_SECONDS = 30

def ensure_directory(path):
    os.makedirs(path, exist_ok=True)

def flush_batch(records):
    if not records: return
    ts = datetime.utcnow()
    date_str, hour_str = ts.strftime("%Y-%m-%d"), ts.strftime("%H")
    df = pd.DataFrame(records)
    df["ingestion_date"] = date_str
    df["ingestion_hour"] = hour_str
    df["ingested_at"] = ts.isoformat()
    
    path = os.path.join(BRONZE_PATH, f"ingestion_date={date_str}", f"ingestion_hour={hour_str}")
    ensure_directory(path)
    file_name = f"batch_{int(time.time())}.parquet"
    df.to_parquet(os.path.join(path, file_name), index=False)
    print(f"Bronze written: {len(df)} rows")

def main():
    consumer = KafkaConsumer(
        TOPIC_NAME, bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID, auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )
    batch = []
    start_time = time.time()
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            for _, msgs in msg_pack.items():
                for m in msgs: batch.append(m.value)
            
            if time.time() - start_time >= BATCH_SECONDS:
                flush_batch(batch)
                consumer.commit()
                batch, start_time = [], time.time()
    except KeyboardInterrupt:
        if batch: flush_batch(batch); consumer.commit()
    finally:
        consumer.close()

if __name__ == "__main__":
    main()