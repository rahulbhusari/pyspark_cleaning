from pyspark.sql import SparkSession, functions as F

# 1) import to_date method from functions
from pyspark.sql.functions import *


spark = SparkSession.builder.master("local").getOrCreate()

cleaned_titles_df = spark.read.csv(
    path="./data/final_cleaned_netflix_titles_data.csv", sep="\t", header=True
)

cleaned_titles_df.select("date_added").show(3, truncate=False)
"""
+-------------+
|date_added   |
+-------------+
|July 13, 2017|
|July 13, 2017|
|July 13, 2017|
+-------------+

"""
# 2) create a new column
formatted_date_df = cleaned_titles_df.withColumn(
    "formatted_date",
    from_unixtime(unix_timestamp(col("date_added"), "MMMM d, yyyy"), "MM-dd-yyyy"),
)

formatted_date_df.select("formatted_date").show(10, truncate=False)

"""
+--------------+
|formatted_date|
+--------------+
|07-13-2017    |
|07-13-2017    |
|07-13-2017    |
|07-12-2019    |
|07-12-2019    |
|07-12-2019    |
|07-12-2019    |
|07-12-2019    |
|07-12-2019    |
|07-12-2019    |
+--------------+

"""
