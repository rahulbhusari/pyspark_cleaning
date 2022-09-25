from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()

films_df = spark.read.csv(path="data/films/films.csv", sep=",", header=True)
people_df = spark.read.csv(path="data/films/people.csv", sep=",", header=True)
reviews_df = spark.read.csv(path="data/films/reviews.csv", sep=",", header=True)
roles_df = spark.read.csv(path="data/films/roles.csv", sep=",", header=True)

# remove duplicate titles
films_df = films_df.dropDuplicates(["title"])

films_df.createTempView("films")
people_df.createTempView("people")
reviews_df.createTempView("reviews")
roles_df.createTempView("roles")


query = """
SELECT CAST(AVG(CAST(budget AS INT)) AS INT) average_budget, certification
FROM films
WHERE certification IS NOT NULL
GROUP BY certification
ORDER BY average_budget DESC

"""
spark.sql(f"{query}").show(100, truncate=False)
"""
+--------------+-------------+
|average_budget|certification|
+--------------+-------------+
|51606422      |PG-13        |
|46975783      |PG           |
|44999130      |G            |
|24103538      |R            |
|7940714       |NC-17        |
|5550000       |GP           |
|5232799       |Unrated      |
|4700000       |M            |
|4507385       |Not Rated    |
|4124974       |Approved     |
|3466666       |X            |
|3031420       |Passed       |
+--------------+-------------+
"""
