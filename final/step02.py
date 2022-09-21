from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.master("local").getOrCreate()

cleaned_titles_df = spark.read.csv(
    path="./data/final_cleaned_netflix_titles_data.csv", header=True, sep="\t"
)

cast_members_df = cleaned_titles_df.withColumn(
    "cast_count", F.size(F.split(cleaned_titles_df.cast, ","))
)

# top 10 titles with the largest cast member population
cast_members_df.select("title", "cast_count").orderBy(
    cast_members_df.cast_count.desc()
).show(10, truncate=True)

"""
+--------------------+----------+
|               title|cast_count|
+--------------------+----------+
|        Black Mirror|        50|
|COMEDIANS of the ...|        47|
|         Creeped Out|        47|
|    Arthur Christmas|        44|
|              Narcos|        42|
|Dolly Parton's He...|        41|
|Michael Bolton's ...|        41|
|American Horror S...|        40|
|Love, Death & Robots|        40|
|            Movie 43|        39|
+--------------------+----------+
only showing top 10 rows

"""
