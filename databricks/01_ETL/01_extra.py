from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


mldf = spark.read.option("multiline", "true").json("databricks/data/kafka-events.json")
mldf.printSchema()
"""
root
 |-- key: string (nullable = true)
 |-- offset: string (nullable = true)
 |-- partition: string (nullable = true)
 |-- timestamp: string (nullable = true)
 |-- topic: string (nullable = true)
 |-- value: string (nullable = true)

"""
