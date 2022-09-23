from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


spark = SparkSession.builder.master("local").getOrCreate()

seasons_only_df = spark.read.csv(
    path="./data/final_seasons_only_titles.csv", header=True, sep="\t"
)

# display the name of the shows with double digit season length

# 1) type dast duration to IntegerType
seasons_only_df = seasons_only_df.withColumn(
    "duration_temp", seasons_only_df.duration.cast(IntegerType())
)

# 2) drop the duration column and rename temp
seasons_only_df = seasons_only_df.drop("duration").withColumnRenamed(
    "duration_temp", "duration"
)

# 3) filter out where season length is 10  and above
seasons_only_df.select("title", "duration").where("duration >= 10").orderBy(
    seasons_only_df.duration.desc()
).show(truncate=False)

"""
+--------------------------------+--------+
|title                           |duration|
+--------------------------------+--------+
|Grey's Anatomy                  |15      |
|NCIS                            |15      |
|Supernatural                    |14      |
|COMEDIANS of the world          |13      |
|Trailer Park Boys               |12      |
|Criminal Minds                  |12      |
|Heartland                       |11      |
|Cheers                          |11      |
|Dad's Army                      |10      |
|Danger Mouse: Classic Collection|10      |
+--------------------------------+--------+

"""
