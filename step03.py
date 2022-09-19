from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local").appName("step3").getOrCreate()

titles_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="\t", header=False)

# find the null columns
titles_df = titles_df.filter(F.col("_c0").cast("int").isNull()).show(truncate=True)


"""
ISSUES:
    1)Comment rows - These begin with a # character in the first column, and all other columns are null
    2)Missing first column - We have few rows that reference TV Show or Movie, which should be the 2nd column.
    3)Odd columns - There are a few rows included where the columns seem out of sync
    (ie, a content type in the ID field, dates in the wrong column, etc).
"""
