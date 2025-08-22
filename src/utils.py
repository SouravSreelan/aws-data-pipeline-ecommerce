import pandera as pa
from pandera import Check, Column, DataFrameSchema
import numpy as np
import hashlib
import logging

logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO)

def hash_record(row):
    return hashlib.md5(str(row).encode()).hexdigest()

order_schema = DataFrameSchema({
    "customer_id": Column(int, checks=Check.ge(1)),
    "order_amount": Column(float, checks=[Check.ge(0), Check(lambda s: s.mean() > 10)]),
    "order_date": Column(str, checks=Check.str_matches(r'\d{4}-\d{2}-\d{2}')),
})

def detect_anomalies(df, col='order_amount', z_thresh=3):
    mean = np.mean(df[col])
    std = np.std(df[col])
    z_scores = (df[col] - mean) / std
    df['anomaly'] = np.abs(z_scores) > z_thresh
    logging.info(f"Found {df['anomaly'].sum()} anomalies in {col}")
    return df