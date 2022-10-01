from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import Row, IntegerType, DateType

spark = SparkSession.builder.master("local").getOrCreate()

# create a Spark Context object
sc = spark.sparkContext

# create RDD
dirty_sales_data_RDD = sc.textFile("data/dirty_sales_data.csv")

# save the first row as header
header = dirty_sales_data_RDD.first()

# filter out the header
filtered_sales_data_RDD = dirty_sales_data_RDD.filter(lambda x: x != header)

# split rows into list
# REMEMBER under the hood DF requires and RDD: a list of Row/tuple/list/pandasDF
final_RDD = filtered_sales_data_RDD.map(lambda x: x.split(","))


# convert the RDD to DF
dirty_sales_data_df = final_RDD.toDF()

# dirty_sales_data_df.show(2)

"""
+-------+---+----+---+--------+
|     _1| _2|  _3| _4|      _5|
+-------+---+----+---+--------+
|   John|  1|$100|000|8/1/2022|
|Melissa|  2|$250|000|8/1/2022|
+-------+---+----+---+--------+
"""
