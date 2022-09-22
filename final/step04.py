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

print(formatted_date_df.count())  # 6113


null_date_count = (
    formatted_date_df.select("formatted_date").where("formatted_date is null").count()
)

print(null_date_count)  # 14


# remove titles with null date values
clean_titles_df = formatted_date_df.filter(formatted_date_df.formatted_date != "null")

print(clean_titles_df.count())  # 6099
