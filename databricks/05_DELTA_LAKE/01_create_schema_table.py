from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()

table_name = "managed_table_default_location"
table_location = spark.sql(f"DESCRIBE DETAIL {table_name}").first().location
print(table_location)
# dbfs:/user/hive/warehouse/benkaan001_jprk_dbacademy_delp_default_location.db/managed_table_default_location
