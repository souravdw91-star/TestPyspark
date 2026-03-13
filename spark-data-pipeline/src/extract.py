import os

def read_sales_data(spark):

    path = os.path.abspath("spark-data-pipeline/data/raw/sales.csv")

    df = spark.read \
        .option("header", "true") \
        .csv(path)

    return df