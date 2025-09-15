import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, when
from pyspark.sql.window import Window


if __name__ == "__main__":
    print("Connecting to Spark...")

    spark = SparkSession.builder.master("spark://localhost:7077").getOrCreate()

    print("Spark connection established")

    fib_count = 10 if len(sys.argv) < 2 else int(sys.argv[1])

    fib_df = spark.createDataFrame([(i, ) for i in range(1, fib_count + 1)], ["fib_position"])

    window = Window.orderBy("fib_position")

    fib_df = fib_df.withColumn(
        "fibonacci",
        when(col("fib_position") == 1, 0)
        .when(col("fib_position") == 2, 1)
        .otherwise(None)
    )

    for i in range(3, fib_count + 1):
        fib_df = fib_df.withColumn(
            "fibonacci",
            when(col("fib_position") == i,
                 lag("fibonacci", 1).over(window) + lag("fibonacci", 2).over(window))
            .otherwise(col("fibonacci"))
        )

    print(fib_df.show(n=1000))

    spark.stop()

    print("Connection closed")