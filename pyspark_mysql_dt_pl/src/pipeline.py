import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config_reader import load_config
from config.spark_config import create_spark_session
from src.extract import read_sales_data
from src.transform import transform_sales
from src.load import write_to_mysql
from src.logger import get_logger

logger = get_logger()


def run_pipeline():

    logger.info("Pipeline started")

    config = load_config()

    spark = create_spark_session()

    df = read_sales_data(spark, config)

    logger.info(f"Records read: {df.count()}")

    df_transformed = transform_sales(df)

    write_to_mysql(df_transformed, config)

    logger.info("Pipeline completed successfully")

    spark.stop()


if __name__ == "__main__":
    run_pipeline()