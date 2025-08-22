import pandas as pd
import numpy as np
import yaml
import os

with open('src/config.yaml') as f:
    config = yaml.safe_load(f)
size = config['pipeline']['data_size']

def generate_orders(chunk_size=10000):
    for i in range(1 if size == 'small' else 10):
        data = pd.DataFrame({
            'customer_id': np.random.randint(1, 1000, chunk_size),
            'order_amount': np.random.uniform(10, 500, chunk_size) + np.random.normal(0, 50, chunk_size),
            'order_date': pd.date_range('2025-01-01', periods=chunk_size).strftime('%Y-%m-%d')
        })
        yield data

def generate_clicks(chunk_size=10000):
    for i in range(1 if size == 'small' else 10):
        data = pd.DataFrame({
            'user_id': np.random.randint(1, 1000, chunk_size),
            'click_event': np.random.choice(['view', 'cart', 'purchase'], chunk_size),
            'timestamp': pd.Timestamp.now().isoformat()
        })
        yield data.to_json(orient='records', lines=True)