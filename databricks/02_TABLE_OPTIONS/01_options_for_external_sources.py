from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


spark.sql(
    """
CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales.csv"
"""
)

spark.sql("SELECT COUNT(*) old_count FROM sales_csv").show()
"""
+---------+
|old_count|
+---------+
|      999|
+---------+
"""


# need to direct it to a directory
spark.read.option("header", "true").option("delimiter", "|").csv(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales.csv"
).write.mode("append").format("csv").save(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data"
)
