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

# replace the old date column with new one while keeping the name intact
clean_titles_df = clean_titles_df.drop("date_added").withColumnRenamed(
    "formatted_date", "date_added"
)

clean_titles_df.printSchema()

"""
 |-- show_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- director: string (nullable = true)
 |-- cast: string (nullable = true)
 |-- country: string (nullable = true)
 |-- release_year: string (nullable = true)
 |-- rating: string (nullable = true)
 |-- duration: string (nullable = true)
 |-- listed_in: string (nullable = true)
 |-- description: string (nullable = true)
 |-- type: string (nullable = true)
 |-- date_added: string (nullable = true)

"""
