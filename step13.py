from tokenize import String
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

# imports for the UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


spark = SparkSession.builder.master("local").getOrCreate()

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

titles_clean_df = titles_clean_df.drop("_c0", "splitcolumn", "fieldcount")

# create a UDF
def deriveType(showtype, showduration):
    if showtype == "Movie" or showtype == "TV Show":
        return showtype
    else:
        if showduration.endswith("min"):
            return "Movie"
        else:
            return "TV Show"


udfDeriveType = udf(deriveType, returnType=StringType())


titles_clean_df = titles_clean_df.withColumn(
    "derivedType", udfDeriveType(F.col("type"), F.col("duration"))
)

titles_clean_df = titles_clean_df.drop("type").withColumnRenamed("derivedType", "type")


# spark is optimized to take advantage of Parquet
# titles_clean_df.write.parquet("./data/netflix_titles_cleaned.parquet", mode='overwrite')

# Coalesce and save the data in CSV format
titles_clean_df.coalesce(1).write.csv(
    "./data/cleaned_netflix_titles.csv", mode="overwrite", sep="\t", header=True
)

# Rename the file in the terminal using the below relative path
"""
mv data/cleaned_netflix_titles.csv/part-00000-3bfd1019-dac0-4c40-b1c3-259b83b57569-c000.csv data/final_cleaned_netflix_titles_data.csv

"""
