from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").getOrCreate()


df = spark.createDataFrame([("2019-06-22",)], ["t"])
df1 = df.select(to_date(df.t, "yyyy-MM-dd").alias("dt"))

df1.show(truncate=False)

df = spark.createDataFrame([("December 31, 2022",)], ["date"])

df.show()

"""
+-----------------+
|             date|
+-----------------+
|December 31, 2022|
+-----------------+
"""

df.withColumn(
    "formatted-date",
    from_unixtime(unix_timestamp(col("date"), "MMMM d, yyyy"), "MM-dd-yyyy"),
).show(truncate=False)

"""
+-----------------+--------------+
|date             |formatted-date|
+-----------------+--------------+
|December 31, 2022|12-31-2022    |
+-----------------+--------------+

"""
