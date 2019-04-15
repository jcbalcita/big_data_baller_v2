from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
import pyspark.sql.functions as f

# i don't know how to python, sorry

if __name__ == "__main__":
    sc = SparkContext(appName="parquet")
    sql = SQLContext(sc)

    int_cols = [
            "player_id",
            "player_height_inches",
            "games_played",
            "player_weight",
            "draft_number",
            "draft_round",
            "draft_year"
            ]

    decimal_cols = [
            "age",
            "ast",
            "ast_pct",
            "dreb_pct",
            "oreb_pct",
            "net_rating",
            "pts",
            "reb",
            "ts_pct",
            "usg_pct",
            "pts",
            ]

    schema = StructType([
            StructField("player_id", StringType(), True),
            StructField("player_name", StringType(), True),
            StructField("player_height", StringType(), True),
            StructField("player_height_inches", StringType(), True),
            StructField("player_weight", StringType(), True),
            StructField("age", StringType(), True),
            StructField("college", StringType(), True),
            StructField("country", StringType(), True),
            StructField("draft_number", StringType(), True),
            StructField("draft_round", StringType(), True),
            StructField("draft_year", StringType(), True),
            StructField("games_played", StringType(), True),
            StructField("pts", StringType(), True),
            StructField("reb", StringType(), True),
            StructField("ast", StringType(), True),
            StructField("usg_pct", StringType(), True),
            StructField("ts_pct", StringType(), True),
            StructField("dreb_pct", StringType(), True),
            StructField("oreb_pct", StringType(), True),
            StructField("ast_pct", StringType(), True),
            StructField("net_rating", StringType(), True),
            StructField("team_abbreviation", StringType(), True),
            StructField("team_id", StringType(), True)])

    for year in range(1996, 2019):
        rdd = sc.textFile("csv/player/player_bio_stats/" + str(year) + ".csv").map(lambda line: line.split(","))
        df = sql.createDataFrame(rdd, schema)

        for col in int_cols:
            df = df.withColumn(col, df[col].cast(IntegerType()))

        for col in decimal_cols:
            df = df.withColumn(col, df[col].cast(DecimalType(10,5)))

        df.write.parquet("parquet/player/player_bio_stats/" + str(year))