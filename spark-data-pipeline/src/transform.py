from pyspark.sql.functions import col

def transform_sales(df):

    df = df.withColumn("price", col("price").cast("double")) \
           .withColumn("quantity", col("quantity").cast("int"))

    df_transformed = df.withColumn(
        "total_sales",
        col("price") * col("quantity")
    )

    return df_transformed