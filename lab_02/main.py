from pyspark.sql import SparkSession
from operator import add
from pprint import pprint


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    """
    1 task
    """
    df = (spark.read.
          schema(
            "InvoiceNo STRING,"
            "StockCode STRING,"
            "Description STRING,"
            "Quantity INTEGER,"
            "InvoiceDate STRING,"
            "UnitPrice DOUBLE,"
            "CustomerID STRING,"
            "Country STRING").
          csv(path="data/data.csv", header=True))

    print("\n==========\n| Task 1 |\n==========\n")
    pprint(df.take(5))
    print()

    """
    2 task
    """
    popular_products = (df.rdd
                    .map(lambda row: (row["StockCode"], row["Quantity"] if row["Quantity"] and row["Quantity"] > 0 else 0))
                    .reduceByKey(add)
                    .takeOrdered(5, lambda x: -x[1]))

    print("==========\n| Task 2 |\n==========\n")
    pprint(popular_products)
    print()

    (spark
     .createDataFrame(popular_products, schema=["StockCode", "SoldCount"])
     .coalesce(1)
     .write
     .csv("results/task2", mode="overwrite", header=True))

    """
    3.1 task
    """
    orders_count = (df.rdd
                    .map(lambda row: (row["CustomerID"], 1) if row["CustomerID"] is not None else ("Empty customer ID", 1))
                    .reduceByKey(add)
                    .collect())

    print("============\n| Task 3.1 |\n============\n")
    pprint(orders_count[:5])
    print()

    (spark
     .createDataFrame(orders_count, schema=["CustomerID", "OrdersCount"])
     .coalesce(1)
     .write
     .csv("results/task3.1", mode="overwrite", header=True))

    """
    3.2 task
    """
    total_spent = (df.rdd
                   .map(lambda row: (row["CustomerID"],
                                     row["UnitPrice"] * row["Quantity"]
                                        if row["UnitPrice"] and row["Quantity"] and row["Quantity"] > 0
                                        else 0.))
                   .filter(lambda x: x[0] and x[0] != "")
                   .reduceByKey(add)
                   .collect())

    print("============\n| Task 3.2 |\n============\n")
    pprint(total_spent[:5])
    print()

    (spark
     .createDataFrame(total_spent, schema=["CustomerID", "TotalSpent"])
     .coalesce(1)
     .write
     .csv("results/task3.2", mode="overwrite", header=True))

    """
    3.3 task
    """
    avg_price = (df.rdd
                 .map(lambda row: (row["InvoiceNo"],
                                   (
                                       row["CustomerID"],
                                       row["UnitPrice"] * row["Quantity"]
                                            if row["UnitPrice"] and row["Quantity"] and row["Quantity"] > 0
                                            else 0.
                                   )))
                 .filter(lambda x: x[0] and x[1][0] and x[0] != "" and x[1][0] != "" and x[1][1] > 0)
                 .reduceByKey(lambda x, y: (x[0], x[1] + y[1]))
                 .map(lambda x: (x[1][0], (x[1][1], 1)))
                 .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                 .mapValues(lambda x: x[0] / x[1])
                 .collect())

    print("============\n| Task 3.3 |\n============\n")
    pprint(avg_price[:5])
    print()

    (spark
     .createDataFrame(avg_price, schema=["CustomerID", "AvgInvoice"])
     .coalesce(1)
     .write
     .csv("results/task3.3", mode="overwrite", header=True))

    spark.stop()
