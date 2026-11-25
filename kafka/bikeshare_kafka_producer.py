import json
import time
import pandas as pd
from kafka import KafkaProducer

# ------------------------------------------------------------Configuration------------------------------------------------------------------------------------------------------------------------
BOOTSTRAP_SERVERS = "VMIP:9092" # external IP of VM
TOPIC = "bikeshare_rides" 
CSV_FILE = "C:/Users/Gebruiker/OneDrive/DSBE (master)/Data Engineering/Assignment_2/daily_rent_detail.csv"
DATE = "2024-02-03"
SPEED = 600.0

COLS = ['ride_id', 'rideable_type', 'started_at', 'ended_at',
       'start_station_name', 'start_station_id', 'end_station_name',
       'end_station_id', 'member_casual']

def kafka_python_producer_sync(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8'))
    print("Sending " + msg)
    producer.flush(timeout=60)

def success(metadata):
    print(metadata.topic)

def error(exception):
    print(exception)

def kafka_python_producer_async(producer, msg, topic):
    producer.send(topic, bytes(msg, encoding='utf-8')).add_callback(success).add_errback(error)
    # flush happens one time at the end in produce_from_file (faster than per msg)

def produce_from_file(producer, file):
    print(file)
    df = pd.read_csv(file, usecols=COLS, dtype=str)
    df['_ended_ts'] = pd.to_datetime(df['ended_at'], errors='coerce')
    df = df.dropna(subset=['_ended_ts'])

    if DATE: 
        df = df[df['_ended_ts'].dt.strftime('%Y-%m-%d') == DATE]

    # een rit wordt pas geregistreerd als hij is afgelopen: sorteren op ended_at
    df = df.sort_values('_ended_ts')
    print(f"{len(df)} trips to send to topic '{TOPIC}'")

    prev_ts = None
    for item in df.to_dict('records'):
        ts = item.pop('_ended_ts')
        if SPEED > 0 and prev_ts is not None:
            wait = (ts - prev_ts).total_seconds() / SPEED
            if wait > 0:
                time.sleep(min(wait, 1.0))
        prev_ts = ts
        message = {k: (None if pd.isna(v) else v) for k, v in item.items()}
        kafka_python_producer_async(producer, json.dumps(message), TOPIC)

    producer.flush()

def run_job():
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS)
    produce_from_file(producer, CSV_FILE)
    print("Klaar")

if __name__ == '__main__':
    run_job()

