from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col

spark = SparkSession.builder.master("local").getOrCreate()

# create the sales table
spark.sql(
    """
CREATE TABLE IF NOT EXISTS sales
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = ","
)
LOCATION "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete.csv"
"""
)

# spark.sql("SELECT * FROM sales").show(3)

salesDF = spark.table("sales")
salesDF.show(3)
"""
+--------+--------------------+----------------------+-------------------+-----------------------+------------+--------------------+
|order_id|               email|transactions_timestamp|total_item_quantity|purchase_revenue_in_usd|unique_items|               items|
+--------+--------------------+----------------------+-------------------+-----------------------+------------+--------------------+
|  285712|wbrown@gonzales-m...|      1592521889512254|                  2|                 1071.0|           1|"[{""coupon"":""N...|
|  277921|campbellkatrina@p...|      1592458670364116|                  1|                  850.5|           1|"[{""coupon"":""N...|
|  274566|gregorytorres@mey...|      1592419954844631|                  1|                  535.5|           1|"[{""coupon"":""N...|
+--------+--------------------+----------------------+-------------------+-----------------------+------------+--------------------+
"""

# define UDF
def first_letter_function(email):
    """Returns the first letter of the email that is passed in"""
    return email[0]


# assign UDF
first_letter_udf = udf(first_letter_function)

# apply UDF
salesDF.select(first_letter_udf(col("email")).alias("udf_result")).show(3)

"""
+----------+
|udf_result|
+----------+
|         w|
|         c|
|         g|
+----------+

"""


# register UDF to use in SQL
first_letter_udf = spark.udf.register("sql_udf", first_letter_function)

spark.sql(
    """
SELECT sql_udf(email) as first_letter
FROM sales
"""
).show(3)

"""
+------------+
|first_letter|
+------------+
|           w|
|           c|
|           g|
+------------+
"""
