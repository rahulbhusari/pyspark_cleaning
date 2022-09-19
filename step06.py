from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local").appName("step6").getOrCreate()

titles_single_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="{")

comments_df = titles_single_df.filter(F.col("_c0").startswith("#")).count()
print(comments_df.count())  # 47

# filter out comments from the DF
# notice the use of ~, which Returns the result of bitwise NOT of expr.
titles_single_df = titles_single_df.filter(~F.col("_c0").startswith("#"))
print(titles_single_df)  # 6191
