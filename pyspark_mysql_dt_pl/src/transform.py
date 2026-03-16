"""Transformations applied to the sales dataset.

This module contains logic for normalizing the incoming CSV data and computing
any derived columns needed for downstream consumption.
"""

from pyspark.sql.functions import col


def transform_sales(df):
    """Normalize data types and calculate derived columns.

    This function assumes the incoming DataFrame has at least the following
    columns:
      - price
      - quantity
      - date

    It enforces correct Spark data types before doing arithmetic or date
    operations.
    """

    # Convert string values (from CSV) into numeric/date types so that
    # Spark can perform calculations and date operations correctly.
    df = df.withColumn("price", col("price").cast("double"))
    df = df.withColumn("quantity", col("quantity").cast("double"))
    df = df.withColumn("date", col("date").cast("date"))

    # Add a derived metric for analysis / reporting.
    df = df.withColumn(
        "total_sales",
        col("price") * col("quantity")
    )

    return df