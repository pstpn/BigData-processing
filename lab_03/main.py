from pprint import pprint
from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import StringType
import re


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.text("data/data.txt", lineSep='\n\n')

    def clean_text(text):
        cleaned = re.sub(
            '''\\b[\w\d]{1,3}\\b|[,\(\)\.\d:\\-\+"'\[\]\{\}\*\$\%\#\@\!\?\~`=/;]*''',
            '',
            str(text).replace('\n', '')
        )

        return re.sub(r'\s+', ' ', cleaned).strip()

    clean_text_udf = f.udf(clean_text, StringType())

    words_df = (df
                .withColumn("cleaned_text", clean_text_udf(f.col("value")))
                .drop(f.col("value"))
                .filter(f.col("cleaned_text") != "")
                .withColumn("words", f.split(f.col("cleaned_text"), " "))
                .drop(f.col("cleaned_text"))
                .withColumn("word", f.explode(f.col("words")))
                .drop(f.col("words"))
                .filter(f.col("word") != ""))

    pprint(words_df.rdd
           .map(lambda row: (row['word'], 1))
           .reduceByKey(lambda x, y: x + y)
           .takeOrdered(10, lambda row: -row[1]))

    spark.stop()
