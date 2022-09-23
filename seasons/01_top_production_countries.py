from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.master("local").getOrCreate()

seasons_only_df = spark.read.csv(
    path="./data/final_seasons_only_titles.csv", header=True, sep="\t"
)

# display the top 5 countries by total production size
# ISSUE: Need to split country column with a comma delimiter because some columns have multi countries

seasons_only_df.where(F.size(F.split(seasons_only_df.country, ",")) == 1).groupBy(
    "country"
).agg({"country": "count"}).orderBy(F.count(seasons_only_df.country).desc()).show(
    5, truncate=True
)


"""
+--------------+--------------+
|       country|count(country)|
+--------------+--------------+
| United States|           534|
|United Kingdom|           176|
|         Japan|           124|
|   South Korea|           101|
|        Taiwan|            63|
+--------------+--------------+
only showing top 5 rows

"""
