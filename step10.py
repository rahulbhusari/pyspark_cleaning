from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.master("local").appName("step7").getOrCreate()

titles_single_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="{")

titles_single_df = titles_single_df.filter(~F.col("_c0").startswith("#"))

titles_single_df = titles_single_df.withColumn(
    "fieldcount", F.size(F.split(titles_single_df["_c0"], "\t"))
)

titles_single_df = titles_single_df.where("fieldcount == 12")

titles_clean_df = titles_single_df.select(F.split("_c0", "\t").alias("splitcolumn"))


titles_clean_df = titles_clean_df.withColumn(
    "show_id", titles_clean_df.splitcolumn.getItem(0).cast(IntegerType())
)
titles_clean_df = titles_clean_df.withColumn(
    "type", titles_clean_df.splitcolumn.getItem(1)
)
titles_clean_df = titles_clean_df.withColumn(
    "title", titles_clean_df.splitcolumn.getItem(2)
)
titles_clean_df = titles_clean_df.withColumn(
    "director", titles_clean_df.splitcolumn.getItem(3)
)
titles_clean_df = titles_clean_df.withColumn(
    "cast", titles_clean_df.splitcolumn.getItem(4)
)
titles_clean_df = titles_clean_df.withColumn(
    "country", titles_clean_df.splitcolumn.getItem(5)
)
titles_clean_df = titles_clean_df.withColumn(
    "date_added", titles_clean_df.splitcolumn.getItem(6)
)
titles_clean_df = titles_clean_df.withColumn(
    "release_year", titles_clean_df.splitcolumn.getItem(7).cast(IntegerType())
)
titles_clean_df = titles_clean_df.withColumn(
    "rating", titles_clean_df.splitcolumn.getItem(8)
)
titles_clean_df = titles_clean_df.withColumn(
    "duration", titles_clean_df.splitcolumn.getItem(9)
)
titles_clean_df = titles_clean_df.withColumn(
    "listed_in", titles_clean_df.splitcolumn.getItem(10)
)
titles_clean_df = titles_clean_df.withColumn(
    "description", titles_clean_df.splitcolumn.getItem(11)
)

# drop the columns that are not needed any more
titles_clean_df = titles_clean_df.drop("_c0", "splitcolumn", "fieldcount")

# print out the count
print(titles_clean_df.count())  # 6113

# print the schema
titles_clean_df.printSchema()


"""
root
 |-- show_id: integer (nullable = true)
 |-- type: string (nullable = true)
 |-- title: string (nullable = true)
 |-- director: string (nullable = true)
 |-- cast: string (nullable = true)
 |-- country: string (nullable = true)
 |-- date_added: string (nullable = true)
 |-- release_year: integer (nullable = true)
 |-- rating: string (nullable = true)
 |-- duration: string (nullable = true)
 |-- listed_in: string (nullable = true)
 |-- description: string (nullable = true)

"""
