from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local").appName("step7").getOrCreate()

titles_single_df = spark.read.csv(path="./data/netflix_titles_dirty*", sep="{")

titles_single_df = titles_single_df.filter(~F.col("_c0").startswith("#"))


titles_single_df = titles_single_df.withColumn(
    "fieldcount", F.size(F.split(titles_single_df["_c0"], "\t"))
)

# set the DataFrame without bad rows
titles_single_df = titles_single_df.where("fieldcount == 12")

# -------------------------------------------------------------------------------------------
# create a new column that is a list containing all columns and give an alias as splitColumn
titles_clean_df = titles_single_df.select(F.split("_c0", "\t").alias("splitcolumn"))

titles_clean_df.show(truncate=False)
