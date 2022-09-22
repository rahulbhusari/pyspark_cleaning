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

# typecast show_id column
clean_titles_df = clean_titles_df.withColumn(
    "show_id_temp", clean_titles_df.show_id.cast(IntegerType())
)

# replace and rename the show_id column
clean_titles_df = clean_titles_df.drop("show_id").withColumnRenamed(
    "show_id_temp", "show_id"
)

# typecast release_year column
clean_titles_df = clean_titles_df.withColumn(
    "release_year_temp", clean_titles_df.release_year.cast(IntegerType())
)

# replace and rename the release_year column
clean_titles_df = clean_titles_df.drop("release_year").withColumnRenamed(
    "release_year_temp", "release_year"
)

# typecast date_added column
clean_titles_df = clean_titles_df.withColumn(
    "date_added_temp", clean_titles_df.date_added.cast(DateType())
)

# replace and rename the date_added column
clean_titles_df = clean_titles_df.drop("date_added").withColumnRenamed(
    "date_added_temp", "date_added"
)

# print schema
clean_titles_df.printSchema()

"""
root
 |-- title: string (nullable = true)
 |-- director: string (nullable = true)
 |-- cast: string (nullable = true)
 |-- country: string (nullable = true)
 |-- rating: string (nullable = true)
 |-- duration: string (nullable = true)
 |-- listed_in: string (nullable = true)
 |-- description: string (nullable = true)
 |-- type: string (nullable = true)
 |-- show_id: integer (nullable = true)
 |-- release_year: integer (nullable = true)
 |-- date_added: date (nullable = true)

"""
