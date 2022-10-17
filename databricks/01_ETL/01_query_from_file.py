from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()


# multi-line mode
spark.sql(
    """
CREATE TEMPORARY VIEW events_view
USING json
OPTIONS (path="databricks/data/kafka-events.json",multiline=true)
"""
)

spark.sql("SELECT COUNT(*) rows FROM events_view").show()

"""
+----+
|rows|
+----+
| 321|
+----+

"""

mldf = spark.read.option("multiline", "true").json("databricks/data/kafka-events.json")
mldf.printSchema()
