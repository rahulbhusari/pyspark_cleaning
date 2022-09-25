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

# list movies that has an imdb_score above 8.0 and
# the same budget as those that have above 9.0 imdb_score
# list them by descending imdb_score
query = """
SELECT f1.title, f1.budget, r1.imdb_score
FROM roles r
JOIN people p
ON p.id = r.person_id
LEFT JOIN films f1
ON f1.id = r.film_id
JOIN reviews r1
ON r1.film_id = f1.id
WHERE f1.budget IN
    (SELECT f.budget
    FROM films f
    JOIN reviews r
    ON f.id = r.film_id
    WHERE imdb_score > 9.0 AND f.budget IS NOT NULL)
AND r1.imdb_score IN
    (SELECT r.imdb_score
    FROM films f
    JOIN reviews r
    ON f.id = r.film_id
    WHERE imdb_score > 8.0 AND f.budget IS NOT NULL)
GROUP BY f1.title, f1.budget, r1.imdb_score
ORDER BY r1.imdb_score DESC
"""

spark.sql(f"{query}").show(100, truncate=False)


"""
+----------------------------------+--------+----------+
|title                             |budget  |imdb_score|
+----------------------------------+--------+----------+
|The Shawshank Redemption          |25000000|9.3       |
|The Godfather                     |6000000 |9.2       |
|Kickboxer: Vengeance              |17000000|9.1       |
|Goodfellas                        |25000000|8.7       |
|The Usual Suspects                |6000000 |8.6       |
|Snatch                            |6000000 |8.3       |
|Scarface                          |25000000|8.3       |
|Metropolis                        |6000000 |8.3       |
|Warrior                           |25000000|8.2       |
|The Help                          |25000000|8.1       |
|No Country for Old Men            |25000000|8.1       |
|There Will Be Blood               |25000000|8.1       |
|The Grand Budapest Hotel          |25000000|8.1       |
|Butch Cassidy and the Sundance Kid|6000000 |8.1       |
|Platoon                           |6000000 |8.1       |
+----------------------------------+--------+----------+

"""
