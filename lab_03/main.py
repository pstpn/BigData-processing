from pyspark.sql import SparkSession
import re


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    df = spark.read.text("data/data.txt", lineSep='\n\n')

    # TODO: delete
    print(df.count())
    df.show(50)

    filtered_rdd = (df.rdd
                   .map(lambda x: re.
                        sub('''\\b[\w\d]{1,3}\\b|[,\(\)\.\d:\\-\+"'\[\]\{\}\*\$\%\#\@\!\?\~`=/;]*''',
                            '',
                            str(x['value']).replace('\n', '')).
                        replace('  ', ' ').
                        replace('  ', ' '))
                   .filter(lambda x: str(x).replace(' ', '') != ''))

    # TODO: delete
    print(filtered_rdd.take(100))

    spark.stop()
