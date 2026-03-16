from pyspark.sql.functions import col


def transform_sales(df):

    # Ensure numeric types before arithmetic operations
    df = df.withColumn("price", col("price").cast("double"))
    df = df.withColumn("quantity", col("quantity").cast("double"))
    df = df.withColumn("date", col("date").cast("date"))

    df = df.withColumn(
        "total_sales",
        col("price") * col("quantity")
    )

    return df