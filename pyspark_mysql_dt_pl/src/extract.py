def read_sales_data(spark, config):

    path = config["data"]["input_csv"]

    # Let Spark infer numeric/boolean/date types from the CSV content.
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(path)

    return df