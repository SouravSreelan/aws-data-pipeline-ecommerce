from pyspark.sql import SparkSession
import pytest

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_etl(spark):
    df = spark.createDataFrame([(1, 100.0, '2025-01-01')], ["customer_id", "order_amount", "order_date"])
    assert df.count() == 1