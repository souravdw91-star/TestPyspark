def read_sales_data(spark, config):
    """Read sales data from a CSV file into a Spark DataFrame.

    Schema inference is enabled so that numeric/date columns are read as the
    correct Spark types (e.g., DoubleType, DateType).

    Args:
        spark: SparkSession instance.
        config: Loaded configuration dict.
    """

    path = config["data"]["input_csv"]

    # Let Spark infer numeric/boolean/date types from the CSV content.
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)

    return df