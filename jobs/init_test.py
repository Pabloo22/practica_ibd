from pyspark.sql import SparkSession


def first_test():
    spark = SparkSession.builder.appName("SinglePartitionExample").getOrCreate()
    data = [(1, "Alice", 25),
            (2, "Bob", 30),
            (3, "Charlie", 22)]

    schema = ["id", "name", "age"]
    df = spark.createDataFrame(data, schema=schema)

    print(df.take(1))
    print(df.collect())

    spark.stop()


if __name__ == '__main__':
    first_test()