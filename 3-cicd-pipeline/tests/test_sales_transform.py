import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from sales_transform import clean_sales_data, add_revenue_bucket, aggregate_by_region, run_transform


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("region", StringType(), True),
    StructField("revenue", DoubleType(), True),
])


def test_clean_drops_null_order_id(spark):
    df = spark.createDataFrame([(None, "APAC ", 500.0), ("O1", "APAC ", 500.0)], SCHEMA)
    result = clean_sales_data(df)
    assert result.count() == 1


def test_clean_drops_negative_revenue(spark):
    df = spark.createDataFrame([("O1", "APAC", -50.0), ("O2", "APAC", 50.0)], SCHEMA)
    result = clean_sales_data(df)
    assert result.count() == 1


def test_clean_trims_region_whitespace(spark):
    df = spark.createDataFrame([("O1", "  APAC  ", 500.0)], SCHEMA)
    result = clean_sales_data(df).collect()[0]
    assert result["region"] == "APAC"


def test_revenue_bucket_low(spark):
    df = spark.createDataFrame([("O1", "APAC", 500.0)], SCHEMA)
    result = add_revenue_bucket(df).collect()[0]
    assert result["revenue_bucket"] == "Low"


def test_revenue_bucket_medium(spark):
    df = spark.createDataFrame([("O1", "APAC", 5000.0)], SCHEMA)
    result = add_revenue_bucket(df).collect()[0]
    assert result["revenue_bucket"] == "Medium"


def test_revenue_bucket_high(spark):
    df = spark.createDataFrame([("O1", "APAC", 50000.0)], SCHEMA)
    result = add_revenue_bucket(df).collect()[0]
    assert result["revenue_bucket"] == "High"


def test_aggregate_by_region(spark):
    df = spark.createDataFrame([
        ("O1", "APAC", 1000.0),
        ("O2", "APAC", 3000.0),
        ("O3", "EU", 2000.0),
    ], SCHEMA)
    result = aggregate_by_region(df).collect()
    apac_row = [r for r in result if r["region"] == "APAC"][0]
    assert apac_row["total_revenue"] == 4000.0
    assert apac_row["order_count"] == 2


def test_run_transform_end_to_end(spark):
    df = spark.createDataFrame([
        (None, "APAC", 500.0),
        ("O2", "APAC", -10.0),
        ("O3", " EU ", 15000.0),
    ], SCHEMA)
    result = run_transform(df)
    assert result.count() == 1
    row = result.collect()[0]
    assert row["region"] == "EU"
    assert row["revenue_bucket"] == "High"
