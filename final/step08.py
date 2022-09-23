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

# 1)filter out seasons to a new dataframe
seasons_only_df = clean_titles_df.where("duration LIKE '%Season%'")

print(seasons_only_df.count())  # 1926

# 2) create a new colum with integer value of the season duration
seasons_only_df = seasons_only_df.withColumn(
    "season_volume", F.split("duration", " ")[0].cast(IntegerType())
)

# 3) drop columns no longer needed
seasons_only_df = seasons_only_df.drop("duration", "type").withColumnRenamed(
    "season_volume", "duration"
)

# 4) save the data in CSV format
seasons_only_df.coalesce(1).write.csv(
    path="./data/seasons_only_titles.csv", mode="overwrite", sep="\t", header=True
)

# 5) move the csv file
""" mv data/seasons_only_titles.csv/part-00000-f502b448-7a95-48ed-bfbb-c9a208931d8a-c000.csv data/final_seasons_only_titles.csv """
