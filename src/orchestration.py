import yaml
import threading
import time
import os
import logging
from src.ingestion import ingest_batch, ingest_stream
from src.etl import etl_process

logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO)

with open('src/config.yaml') as f:
    config = yaml.safe_load(f)

def orchestrate():
    start = time.time()
    output_path = f"{config['local']['data_dir']}/processed"
    if os.path.exists(output_path):
        logging.info("Skipping: Output exists")
        return
    for attempt in range(3):
        try:
            ingest_thread = threading.Thread(target=lambda: (ingest_batch(), ingest_stream()))
            etl_thread = threading.Thread(target=etl_process)
            ingest_thread.start()
            ingest_thread.join()
            etl_thread.start()
            etl_thread.join()
            logging.info(f"Pipeline ran in {time.time() - start}s")
            break
        except Exception as e:
            logging.error(f"Attempt {attempt+1} failed: {e}")
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)

if __name__ == '__main__':
    orchestrate()