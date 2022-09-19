from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local").appName("step7").getOrCreate()

titles_single_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="{")

titles_single_df = titles_single_df.filter(~F.col("_c0").startswith("#"))


# check how many columns we have in the data set and take care of the outliers
# -------------------------------------------------------------------------------------------------------
# .withColumn(): Creates a new dataframe with a given column.
# F.size(): Returns the size (length) of a Spark ArrayType column.
# F.split(): Splitting the contents of a dataframe column on a specified character into a Spark ArrayType column

# add a column named fieldCount to display the number of items present in each row
titles_single_df = titles_single_df.withColumn(
    "fieldCount", F.size(F.split(titles_single_df["_c0"], "\t"))
)

# filter out rows that have more than 12 columns
titles_single_df.select("fieldCount", "_c0").where("fieldCount > 12").show(
    truncate=False
)

# filter out rows that have less than 12 columns
titles_single_df.select("fieldCount", "_c0").where("fieldCount < 12").show(
    truncate=False
)

# save these outliers to a seperate DataFrame
titles_badrows_df = titles_single_df.where("fieldCount != 12")

# check the number of bad rows
count = titles_badrows_df.count()
print(count)  # 78

# set the DataFrame without bad rows
titles_single_df = titles_single_df.where("fieldCount == 12")

# check the number of rows in our clean DataFrame
print(titles_single_df.count())  # 6113
