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

query = """
SELECT RANK() OVER(ORDER BY (f.gross - f.budget) DESC) AS rank,
f.gross, f.budget, (f.gross - f.budget) return, f.title, r.imdb_score
FROM films f
INNER JOIN reviews r
ON r.film_id = f.id
ORDER BY (f.gross - f.budget) DESC
LIMIT 20
"""

spark.sql(f"{query}").show(100, truncate=False)

"""
+----+---------+---------+---------+---------------------------------------------+----------+
|rank|gross    |budget   |return   |title                                        |imdb_score|
+----+---------+---------+---------+---------------------------------------------+----------+
|1   |936627416|245000000|691627416|Star Wars: Episode VII - The Force Awakens   |8.1       |
|2   |760505847|237000000|523505847|Avatar                                       |7.9       |
|3   |652177271|150000000|502177271|Jurassic World                               |7.0       |
|4   |658672302|200000000|458672302|Titanic                                      |7.7       |
|5   |460935665|11000000 |449935665|Star Wars: Episode IV - A New Hope           |8.7       |
|6   |434949459|10500000 |424449459|E.T. the Extra-Terrestrial                   |7.9       |
|7   |623279547|220000000|403279547|The Avengers                                 |8.1       |
|8   |422783777|45000000 |377783777|The Lion King                                |8.5       |
|9   |474544677|115000000|359544677|Star Wars: Episode I - The Phantom Menace    |6.5       |
|10  |533316061|185000000|348316061|The Dark Knight                              |9.0       |
|11  |407999255|78000000 |329999255|The Hunger Games                             |7.3       |
|12  |363024263|58000000 |305024263|Deadpool                                     |8.1       |
|13  |424645577|130000000|294645577|The Hunger Games: Catching Fire              |7.6       |
|14  |356784000|63000000 |293784000|Jurassic Park                                |8.1       |
|15  |368049635|76000000 |292049635|Despicable Me 2                              |7.5       |
|16  |350123553|58800000 |291323553|American Sniper                              |7.3       |
|17  |380838870|94000000 |286838870|Finding Nemo                                 |8.2       |
|18  |436471036|150000000|286471036|Shrek 2                                      |7.2       |
|19  |377019252|94000000 |283019252|The Lord of the Rings: The Return of the King|8.9       |
|20  |309125409|32500000 |276625409|Star Wars: Episode VI - Return of the Jedi   |8.4       |
+----+---------+---------+---------+---------------------------------------------+----------+

"""
