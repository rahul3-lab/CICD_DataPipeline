"""
Sales data transformation module.
Extracted as a standalone, testable unit so it can be exercised by the
CI/CD pipeline (unit tests run automatically on every push/PR before
deployment to dev/test/prod).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_sales_data(df: DataFrame) -> DataFrame:
    """Drop rows with null order_id or negative revenue; trim whitespace in region."""
    return (
        df.filter(F.col("order_id").isNotNull())
          .filter(F.col("revenue") >= 0)
          .withColumn("region", F.trim(F.col("region")))
    )


def add_revenue_bucket(df: DataFrame) -> DataFrame:
    """Bucket each order into Low / Medium / High revenue tiers."""
    return df.withColumn(
        "revenue_bucket",
        F.when(F.col("revenue") < 1000, "Low")
         .when(F.col("revenue") < 10000, "Medium")
         .otherwise("High")
    )


def aggregate_by_region(df: DataFrame) -> DataFrame:
    """Total and average revenue per region, ordered by total revenue desc."""
    return (
        df.groupBy("region")
          .agg(
              F.sum("revenue").alias("total_revenue"),
              F.avg("revenue").alias("avg_revenue"),
              F.count("order_id").alias("order_count"),
          )
          .orderBy(F.desc("total_revenue"))
    )


def run_transform(df: DataFrame) -> DataFrame:
    """Full pipeline: clean -> bucket -> return cleaned+bucketed records."""
    cleaned = clean_sales_data(df)
    return add_revenue_bucket(cleaned)
