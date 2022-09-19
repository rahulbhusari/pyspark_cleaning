from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local").appName("step3").getOrCreate()

titles_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="\t", header=False)

# Manipulate the csv loading method by providing a custom seperator that is not present in the dataset
# Load the files into a DataFrame with a single column - notice that { does not exist
titles_single_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="{")


# count rows
print(titles_single_df.count())  # 6238


# show header

print(titles_single_df.show(truncate=False))


# print schema
titles_single_df.printSchema()
"""
root
 |-- _c0: string (nullable = true)
"""
