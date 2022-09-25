from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").getOrCreate()

films_df = spark.read.csv(path="data/films/films.csv", sep=",", header=True)
people_df = spark.read.csv(path="data/films/people.csv", sep=",", header=True)
reviews_df = spark.read.csv(path="data/films/reviews.csv", sep=",", header=True)
roles_df = spark.read.csv(path="data/films/roles.csv", sep=",", header=True)

films_df.createTempView("films")
people_df.createTempView("people")
reviews_df.createTempView("reviews")
roles_df.createTempView("roles")

# ISSUE there are 117 film records that have duplicate title and different values

count = films_df.select("title").count()
print(count)  # 4968
distinct_count = films_df.select("title").distinct().count()
print(distinct_count)  # 4844

# drop duplicate title columns
films_df = films_df.dropDuplicates(["title"])
print(films_df.count())
