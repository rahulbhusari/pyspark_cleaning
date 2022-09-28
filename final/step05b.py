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

clean_titles_df.select("title", "date_added").sort("date_added", ascending=True).show(
    100, truncate=False
)

# calculate the date difference using datediff function
clean_titles_df = clean_titles_df.select(
    "*",
    round(
        datediff(
            from_unixtime(unix_timestamp(current_date(), "MM-dd-yyyy")),
            to_date(col("date_added"), "MM-dd-yyyy"),
        )
        / lit(365),
        0,
    ).alias("date_difference_in_years"),
).sort(col("date_difference_in_years"), ascending=False)

expr = [F.last(col).alias(col) for col in clean_titles_df.columns]
clean_titles_df.agg(*expr).show()

"""
retrieve the last record using last() method
+--------+------------------+--------+--------------------+-------+------------+--------+---------+--------------------+--------------------+-------+----------+---------------+
| show_id|             title|director|                cast|country|release_year|  rating| duration|           listed_in|         description|   type|date_added|date_difference|
+--------+------------------+--------+--------------------+-------+------------+--------+---------+--------------------+--------------------+-------+----------+---------------+
|80186475|Pokémon the Series|    null|Sarah Natochenny,...|  Japan|        2019|TV-Y7-FV|2 Seasons|Anime Series, Kid...|Ash and his Pikac...|TV Show|04-01-2019|            3.0|
+--------+------------------+--------+--------------------+-------+------------+--------+---------+--------------------+--------------------+-------+----------+---------------+
"""
