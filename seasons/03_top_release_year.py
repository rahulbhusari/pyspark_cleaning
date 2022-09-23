from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local").getOrCreate()

seasons_only_df = spark.read.csv(
    path="./data/final_seasons_only_titles.csv", header=True, sep="\t"
)

# display the top 10 years with highest production count

# 1) create a temp column with integer values for release year
seasons_only_df = seasons_only_df.withColumn(
    "release_year_temp", seasons_only_df.release_year.cast(IntegerType())
)

# 2) drop the existing column and rename the temp column
seasons_only_df = seasons_only_df.drop("release_year").withColumnRenamed(
    "release_year_temp", "release_year"
)

# 3) rename the aggreated column, sort by count(release_year)
seasons_only_df.select("release_year").groupBy("release_year").agg(
    {"release_year": "count"}
).withColumnRenamed("count(release_year)", "potato").sort(
    "count(release_year)", ascending=False
).show(
    10
)
