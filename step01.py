from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").appName("data_clean").getOrCreate()

# load data to df using wildcard * to access all files matching _dirty
titles_df = spark.read.csv("./data/netflix_titles_dirty*.csv", header=False, sep="\t")

# print(titles_df.show(150))

titles_df.printSchema()

"""
root
 |-- _c0: string (nullable = true)
 |-- _c1: string (nullable = true)
 |-- _c2: string (nullable = true)
 |-- _c3: string (nullable = true)
 |-- _c4: string (nullable = true)
 |-- _c5: string (nullable = true)
 |-- _c6: string (nullable = true)
 |-- _c7: string (nullable = true)
 |-- _c8: string (nullable = true)
 |-- _c9: string (nullable = true)
 |-- _c10: string (nullable = true)
 |-- _c11: string (nullable = true)

ISSUES:
    1) No schema
    2) Mixed data types in columns
    3) Empty columns


"""
