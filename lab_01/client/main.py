import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def fibonacci_udf(position: int):
    """UDF to calculate nth Fibonacci number"""
    if position < 3:
        return position - 1

    a, b = 1, 1
    for _ in range(3, position):
        a, b = b, a + b

    return str(b)

if __name__ == "__main__":
    print("Connecting to Spark...")

    spark = SparkSession.builder.getOrCreate()

    print("Spark connection established")

    fib_position = 10 if len(sys.argv) < 2 else int(sys.argv[1])

    fib_df = spark.createDataFrame([(fib_position, )], ["fib_position"])
    fib_udf = udf(fibonacci_udf, StringType())
    fib_df = fib_df.withColumn("fib_number", fib_udf("fib_position"))

    print(fib_df.show())

    spark.stop()

    print("Connection closed")