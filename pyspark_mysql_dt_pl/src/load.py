def write_to_mysql(df, config):

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