"""Entry point for the ETL pipeline.

This module wires together extraction, transformation, and loading.
"""

import sys
import os

# Ensure the repo root is on the import path so the package imports work
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config_reader import load_config
from config.spark_config import create_spark_session
from src.extract import read_sales_data
from src.transform import transform_sales
from src.load import write_to_mysql
from src.logger import get_logger

logger = get_logger()


def run_pipeline():
    """Execute the extract-transform-load workflow."""

    logger.info("Pipeline started")

    # Load config and start Spark
    config = load_config()
    spark = create_spark_session()

    # Extract
    df = read_sales_data(spark, config)
    logger.info(f"Records read: {df.count()}")

    # Transform
    df_transformed = transform_sales(df)

    # Load
    write_to_mysql(df_transformed, config)

    logger.info("Pipeline completed successfully")

    # Stop Spark to release resources
    spark.stop()


if __name__ == "__main__":
    run_pipeline()