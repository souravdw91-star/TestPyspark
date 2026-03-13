from pyspark.sql import SparkSession

def create_spark_session():

    spark = (
        SparkSession.builder
        .appName("DataEngineeringPipeline")
        .master("local[*]")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.hadoop.security.authentication", "simple")
        .getOrCreate()
    )

    return spark