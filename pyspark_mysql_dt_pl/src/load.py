def write_to_mysql(df, config):
    """Write a DataFrame to MySQL using JDBC.

    The operation uses overwrite mode by default so the target table is replaced
    on each run.

    Args:
        df: Spark DataFrame to persist.
        config: Configuration dict containing the MySQL connection settings.
    """

    mysql = config["mysql"]

    url = f"jdbc:mysql://{mysql['host']}:{mysql['port']}/{mysql['database']}"

    df.write \
        .format("jdbc") \
        .option("url", url) \
        .option("dbtable", mysql["table"]) \
        .option("user", mysql["user"]) \
        .option("password", mysql["password"]) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .mode("overwrite") \
        .save()