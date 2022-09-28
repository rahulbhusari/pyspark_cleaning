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

# artists who have appeared the most in different films
query = """
SELECT COUNT(f.title) appearance_count, r.person_id, p.name
FROM roles r
INNER JOIN people p
ON p.id = r.person_id
INNER JOIN films f
ON f.id = r.film_id
INNER JOIN reviews rev
ON rev.film_id = f.                                                                                                                                           id
GROUP BY r.person_id, p.name
ORDER BY COUNT(f.title) DESC
"""

spark.sql(f"{query}").show(10, truncate=False)
"""
+----------------+---------+-----------------+
|appearance_count|person_id|name             |
+----------------+---------+-----------------+
|53              |6764     |Robert De Niro   |
|43              |5777     |Morgan Freeman   |
|38              |7492     |Steve Buscemi    |
|38              |1053     |Bruce Willis     |
|37              |5317     |Matt Damon       |
|36              |4022     |Johnny Depp      |
|36              |1500     |Clint Eastwood   |
|33              |910      |Brad Pitt        |
|33              |5941     |Nicolas Cage     |
|33              |2025     |Denzel Washington|
+----------------+---------+-----------------+
only showing top 10 rows

"""
