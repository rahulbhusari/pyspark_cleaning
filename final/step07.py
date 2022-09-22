from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DateType

spark = SparkSession.builder.master("local").getOrCreate()


cleaned_titles_df = spark.read.csv(
    path="./data/final_cleaned_netflix_titles_data.csv", sep="\t", header=True
)

formatted_date_df = cleaned_titles_df.withColumn(
    "formatted_date",
    from_unixtime(unix_timestamp(col("date_added"), "MMMM d, yyyy"), "MM-dd-yyyy"),
)


clean_titles_df = formatted_date_df.filter(formatted_date_df.formatted_date != "null")

clean_titles_df = clean_titles_df.drop("date_added").withColumnRenamed(
    "formatted_date", "date_added"
)


clean_titles_df = clean_titles_df.withColumn(
    "show_id_temp", clean_titles_df.show_id.cast(IntegerType())
)

clean_titles_df = clean_titles_df.drop("show_id").withColumnRenamed(
    "show_id_temp", "show_id"
)

clean_titles_df = clean_titles_df.withColumn(
    "release_year_temp", clean_titles_df.release_year.cast(IntegerType())
)

clean_titles_df = clean_titles_df.drop("release_year").withColumnRenamed(
    "release_year_temp", "release_year"
)


clean_titles_df = clean_titles_df.withColumn(
    "date_added_temp", clean_titles_df.date_added.cast(DateType())
)

clean_titles_df = clean_titles_df.drop("date_added").withColumnRenamed(
    "date_added_temp", "date_added"
)


clean_titles_df = clean_titles_df.withColumn("show_time", F.split("duration", " ")[0])

clean_titles_df.select("duration", "show_time").show(10, truncate=False)
"""
+--------+---------+
|duration|show_time|
+--------+---------+
|59 min  |59       |
|99 min  |99       |
|74 min  |74       |
|105 min |105      |
|1 Season|1        |
|1 Season|1        |
|79 min  |79       |
|1 Season|1        |
|106 min |106      |
|1 Season|1        |
+--------+---------+
only showing top 10 rows

"""

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
