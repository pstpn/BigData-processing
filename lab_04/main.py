from pyspark.sql import SparkSession, functions as f
from pyspark.sql.window import Window


if __name__ == "__main__":
    spark = SparkSession.builder.getOrCreate()

    players_df = spark.read.csv("data/players.csv", header=True, inferSchema=True)
    salaries_df = spark.read.csv("data/salaries_1985to2018.csv", header=True, inferSchema=True)
    seasons_df = spark.read.csv("data/Seasons_Stats.csv", header=True, inferSchema=True)

    efficiency_df = (seasons_df
                     .withColumn("EFF",
                                 f.zeroifnull(f.col("PTS")) +
                                 f.zeroifnull(f.col("TRB")) +
                                 f.zeroifnull(f.col("AST"))))

    efficiency_by_season_df = (efficiency_df
                               .groupBy(['Year', 'Player'])
                               .agg({'EFF': 'sum'})
                               .withColumnRenamed('sum(EFF)', 'EFF'))

    joined_df = (efficiency_by_season_df
                          .join(players_df,
                                players_df['name'] == efficiency_by_season_df['Player'])
                          .join(salaries_df,
                                (salaries_df['player_id'] == players_df['_id']) &
                                (salaries_df['season_start'] == efficiency_by_season_df['Year'])))

    cost_per_season_df = (joined_df
                          .withColumn("CPS", f.try_divide(f.col('salary'), f.col('EFF'))))

    (cost_per_season_df
     .drop('index')
     .write
     .partitionBy('Year')
     .mode('overwrite')
     .format('parquet')
     .save(path='data/results'))

    window = Window.partitionBy("Year").orderBy(f.col("CPS").asc_nulls_last())
    years_df = (spark
     .read
     .parquet('data/results'))

    (years_df
     .drop('index')
     .drop('birthDate')
     .drop('birthPlace')
     .drop('career_AST')
     .drop('career_FG%')
     .drop('career_FG3%')
     .drop('career_FT%')
     .drop('career_G')
     .drop('career_PER')
     .drop('career_PTS')
     .drop('career_TRB')
     .drop('career_WS')
     .drop('career_eFG%')
     .drop('college')
     .drop('draft_pick')
     .drop('draft_round')
     .drop('draft_team')
     .drop('draft_year')
     .drop('height')
     .drop('highSchool')
     .drop('name')
     .drop('position')
     .drop('shoots')
     .drop('weight')
     .drop('league')
     .drop('_id')
     .drop('season')
     .drop('season_end')
     .drop('season_start')
     .drop('team')
     .withColumn('row_number', f.row_number().over(window))
     .filter(f.col('row_number') < 6)
     .show())

    spark.stop()
