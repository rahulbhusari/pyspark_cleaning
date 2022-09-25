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


# figure out the timestampdiff and display current age
query = """
SELECT p.name youngest_actor, f.title, r.role,
ROUND(DATEDIFF(CURRENT_DATE(), p.birthdate)/365) as age
FROM people p
JOIN roles r
ON r.id == p.id
JOIN films f
ON r.film_id == f.id
WHERE p.birthdate ==
    (SELECT MAX(birthdate)
    FROM people)
"""
spark.sql(f"{query}").show(100, truncate=False)

"""
+--------------+----------+-----+----+
|youngest_actor|title     |role |age |
+--------------+----------+-----+----+
|Jacob Tremblay|Waterworld|actor|16.0|
+--------------+----------+-----+----+

"""
