from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local").appName("step3").getOrCreate()


titles_single_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="{")


# filter out the columns that start with # to a new DataFrame
titles_single_new_df = titles_single_df.filter(F.col("_c0").startswith("#"))

titles_single_new_df.show()

print(titles_single_new_df.count())  # 47
