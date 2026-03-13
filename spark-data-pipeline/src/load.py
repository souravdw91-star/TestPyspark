def write_output(df):

    df.coalesce(1).write \
      .mode("overwrite") \
      .option("header", "true") \
      .csv("spark-data-pipeline/data/processed/sales_output")