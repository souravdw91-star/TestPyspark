import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from config.spark_config import create_spark_session
from src.extract import read_sales_data
from src.transform import transform_sales
from src.load import write_output


def run_pipeline():

    spark = create_spark_session()

    df = read_sales_data(spark)

    df_transformed = transform_sales(df)

    write_output(df_transformed)

    spark.stop()


if __name__ == "__main__":
    run_pipeline()