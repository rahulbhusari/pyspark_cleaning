from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local").getOrCreate()

seasons_df = spark.read.csv(
    path="data/final_seasons_only_titles.csv", header=True, sep="\t"
)

seasons_df = seasons_df.withColumn("listed_in_temp", F.split(seasons_df.listed_in, ","))

seasons_df.select(seasons_df.listed_in_temp).show(truncate=False)
