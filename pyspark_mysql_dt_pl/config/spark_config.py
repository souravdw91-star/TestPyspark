"""Spark configuration helpers.

This module centralizes Spark session creation so configuration is easy to adjust
in one place.
"""

from pyspark.sql import SparkSession


def create_spark_session():
    """Create and return a SparkSession configured for this pipeline."""

    spark = (
        SparkSession.builder
        .appName("pyspark_mysql_dt_pl")
        .master("local[*]")
        .config(
            "spark.driver.extraClassPath",
            "jars/mysql-connector-j-8.3.0.jar"
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    return spark