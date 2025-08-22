import yaml
import pandas as pd
import threading
import queue
import os
import logging
from src.utils import order_schema, detect_anomalies, hash_record
from src.data_generator import generate_orders, generate_clicks

with open('src/config.yaml') as f:
    config = yaml.safe_load(f)

stream_queue = queue.Queue()

def save_to_file(df, path):
    df.to_csv(path, index=False)
    return path

def ingest_batch():
    for chunk in generate_orders():
        try:
            order_schema.validate(chunk)
            chunk = detect_anomalies(chunk)
            chunk['hash'] = chunk.apply(hash_record, axis=1)
            path = save_to_file(chunk, f"{config['local']['data_dir']}/orders_{id(chunk)}.csv")
            logging.info(f"Saved batch to {path}")
        except pa.errors.SchemaError as e:
            logging.error(f"Validation error: {e}")

def ingest_stream():
    def producer():
        for json_chunk in generate_clicks():
            for line in json_chunk.splitlines():
                stream_queue.put(line)
    def consumer():
        while True:
            try:
                line = stream_queue.get(timeout=1)
                with open(f"{config['local']['data_dir']}/clicks.jsonl", 'a') as f:
                    f.write(line + '\n')
                stream_queue.task_done()
            except queue.Empty:
                break
    threading.Thread(target=producer).start()
    threading.Thread(target=consumer).start()

if __name__ == '__main__':
    ingest_batch()
    ingest_stream()