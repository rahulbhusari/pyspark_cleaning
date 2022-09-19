from pyspark.sql import SparkSession
from pyspark.sql import functions as F


spark = SparkSession.builder.master("local").appName("data_clean").getOrCreate()

# load data to df using wildcard * to access all files matching _dirty
titles_df = spark.read.csv("./data/netflix_titles_dirty*.csv", header=False, sep="\t")

# print(titles_df.count()) # 6238

# Import the Pyspark SQL helper functions
# Using col() method find the column named _c0
# Typecast string column to int
# Filter out the columns that are not null

titles_df = titles_df.filter(F.col("_c0").cast("int").isNotNull())
print(titles_df.count())  # 6173
