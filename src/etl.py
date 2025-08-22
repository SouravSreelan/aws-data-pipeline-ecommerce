from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number, pandas_udf
from pyspark.sql.types import DoubleType
import yaml
import duckdb
import sqlite3
import logging

logging.basicConfig(filename='logs/pipeline.log', level=logging.INFO)

with open('src/config.yaml') as f:
    config = yaml.safe_load(f)

spark = SparkSession.builder.appName("EcommerceETL") \
    .config("spark.executor.memory", config['spark']['executor_memory']) \
    .config("spark.sql.shuffle.partitions", config['spark']['shuffle_partitions']) \
    .master("local[4]").getOrCreate()

@pandas_udf(DoubleType())
def vectorized_anomaly(series: pd.Series) -> pd.Series:
    mean = series.mean()
    std = series.std()
    return (series - mean) / std

def etl_process():
    # Read
    df_orders = spark.read.csv(f"{config['local']['data_dir']}/orders_*.csv", header=True, inferSchema=True)
    df_clicks = spark.read.json(f"{config['local']['data_dir']}/clicks.jsonl")

    # Join
    small_df = df_clicks.filter(col('click_event') == 'purchase').cache()
    joined = df_orders.join(small_df.hint("broadcast"), df_orders.customer_id == small_df.user_id, "inner")

    # Dedup
    from pyspark.sql.window import Window
    window = Window.partitionBy("customer_id").orderBy("order_date")
    joined = joined.withColumn("row_num", row_number().over(window)).filter(col("row_num") == 1)

    # UDF
    joined = joined.withColumn("z_score", vectorized_anomaly(col("order_amount")))
    logging.info(f"Applied anomaly detection, max z-score: {joined.agg({'z_score': 'max'}).collect()[0][0]}")

    # Aggregate
    agg_df = joined.groupBy("customer_id").agg(sum("order_amount").alias("total_spend"))

    # Save Parquet
    output_path = f"{config['local']['data_dir']}/processed"
    agg_df.write.mode("append").partitionBy(*config['pipeline']['partition_keys']).parquet(output_path)

    # ELT to SQLite
    conn = sqlite3.connect(config['local']['db_path'])
    agg_df.toPandas().to_sql('processed', conn, if_exists='replace', index=False)
    conn.close()

    # DuckDB
    duck_con = duckdb.connect(':memory:')
    duck_con.execute(f"CREATE TABLE processed AS SELECT * FROM parquet_scan('{output_path}/*')")
    logging.info(f"DuckDB table created with {duck_con.execute('SELECT COUNT(*) FROM processed').fetchone()[0]} rows")

if __name__ == '__main__':
    etl_process()
    spark.stop()