from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").getOrCreate()


cleaned_titles_df = spark.read.csv(
    path="./data/final_cleaned_netflix_titles_data.csv", sep="\t", header=True
)

formatted_date_df = cleaned_titles_df.withColumn(
    "formatted_date",
    from_unixtime(unix_timestamp(col("date_added"), "MMMM d, yyyy"), "MM-dd-yyyy"),
)

# remove titles with null date values
clean_titles_df = formatted_date_df.filter(formatted_date_df.formatted_date != "null")


clean_titles_df = clean_titles_df.withColumn("show_time", F.split("duration", " ")[0])


clean_titles_df.select("duration", "show_time").where(
    "duration LIKE '%Season%'"
).distinct().show(truncate=False)

"""
+----------+---------+
|duration  |show_time|
+----------+---------+
|14 Seasons|14       |
|12 Seasons|12       |
|8 Seasons |8        |
|3 Seasons |3        |
|1 Season  |1        |
|2 Seasons |2        |
|13 Seasons|13       |
|11 Seasons|11       |
|5 Seasons |5        |
|9 Seasons |9        |
|7 Seasons |7        |
|4 Seasons |4        |
|6 Seasons |6        |
|15 Seasons|15       |
|10 Seasons|10       |
+----------+---------+

"""
