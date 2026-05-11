from confluent_kafka import Producer
import json
import time
import pandas as pd
import numpy as np

conf = {
    'bootstrap.servers': 'capstroneventhub.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': '*****',
    'client.id': 'day-city-producer'
}
producer = Producer(conf)

def stream_data(file_path, interval, data_type):
    df = pd.read_csv(file_path)
    time_col = 'Datetime' if 'Datetime' in df.columns else 'Date'
    df[time_col] = pd.to_datetime(df[time_col])
    df = df[df[time_col].dt.year == 2024]
    if df.empty:
        print(f"No data found for year 2020 in {file_path}. Exiting.")
        return
    df = df.sort_values(by=time_col)
    grouped = df.groupby(time_col)
    print(f"Starting {data_type} producer...")
    for timestamp, group in grouped:
        for _, row in group.iterrows():
            data = row.replace({np.nan: None}).to_dict()
            data[time_col] = str(data[time_col])
            data['origin_type'] = data_type 
            producer.produce('perdaydata', value=json.dumps(data))
        producer.flush()
        print(f"Sent {len(group)} records for {timestamp}.")
        time.sleep(interval)

if __name__ == "__main__":
    FILE_NAME = 'data/city_day_schema_mismatch.csv'
    INTERVAL = 2
    TYPE = 'CITY_DAY'             
    
    try:
        stream_data(FILE_NAME, INTERVAL, TYPE)
    except KeyboardInterrupt:
        print("\nStopping Producer...")
