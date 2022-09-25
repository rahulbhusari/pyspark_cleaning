from tkinter.tix import Tree
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructField,
    StructType,
    StringType,
    IntegerType,
    FloatType,
    DateType,
)

spark = SparkSession.builder.master("local").getOrCreate()

# define schemas
films_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("release_year", IntegerType(), True),
        StructField("country", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("language", StringType(), True),
        StructField("certification", StringType(), True),
        StructField("gross", IntegerType(), True),
        StructField("budget", IntegerType(), True),
    ]
)

reveiws_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("film_id", IntegerType(), True),
        StructField("num_user", IntegerType(), True),
        StructField("num_critic", IntegerType(), True),
        StructField("imdb_score", FloatType(), True),
        StructField("num_votes", IntegerType(), True),
        StructField("facebook_likes", IntegerType(), True),
    ]
)

roles_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("film_id", IntegerType(), True),
        StructField("person_id", IntegerType(), True),
        StructField("role", StringType(), True),
    ]
)

people_schema = StructType(
    [
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("birthdate", DateType(), True),
        StructField("deathdate", DateType(), True),
    ]
)


reviews_df = spark.read.csv(
    path="data/films/reviews.csv", sep=",", header=True, schema=reveiws_schema
)
people_df = spark.read.csv(
    path="data/films/people.csv", sep=",", header=True, schema=people_schema
)
roles_df = spark.read.csv(
    path="data/films/roles.csv", sep=",", header=True, schema=roles_schema
)
films_df = spark.read.csv(
    path="data/films/films.csv", sep=",", header=True, schema=films_schema
)

# get rid of the 117 duplicate title records in films df
films_df = films_df.dropDuplicates(["title"])

# create temp tables
reviews_df.createTempView("reviews")
films_df.createTempView("films")
people_df.createTempView("people")
roles_df.createTempView("roles")


tables = ["reviews", "films", "people", "roles"]
for table in tables:
    spark.sql(f"DESCRIBE {table}").show(truncate=False)

"""
+--------------+---------+-------+
|col_name      |data_type|comment|
+--------------+---------+-------+
|id            |int      |null   |
|film_id       |int      |null   |
|num_user      |int      |null   |
|num_critic    |int      |null   |
|imdb_score    |float    |null   |
|num_votes     |int      |null   |
|facebook_likes|int      |null   |
+--------------+---------+-------+

+-------------+---------+-------+
|col_name     |data_type|comment|
+-------------+---------+-------+
|id           |int      |null   |
|title        |string   |null   |
|release_year |int      |null   |
|country      |string   |null   |
|duration     |int      |null   |
|language     |string   |null   |
|certification|string   |null   |
|gross        |int      |null   |
|budget       |int      |null   |
+-------------+---------+-------+

+---------+---------+-------+
|col_name |data_type|comment|
+---------+---------+-------+
|id       |int      |null   |
|name     |string   |null   |
|birthdate|date     |null   |
|deathdate|date     |null   |
+---------+---------+-------+

+---------+---------+-------+
|col_name |data_type|comment|
+---------+---------+-------+
|id       |int      |null   |
|film_id  |int      |null   |
|person_id|int      |null   |
|role     |string   |null   |
+---------+---------+-------+
"""
