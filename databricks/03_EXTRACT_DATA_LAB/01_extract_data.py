from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()

"""
field	    type	    description
key	        BINARY	    The user_id field is used as the key; this is a unique alphanumeric field that corresponds to session/cookie information
offset	    LONG	    This is a unique value, monotonically increasing for each partition
partition	INTEGER	    Our current Kafka implementation uses only 2 partitions (0 and 1)
timestamp	LONG	    This timestamp is recorded as milliseconds since epoch, and represents the time at which the producer appends a record to a partition
topic	    STRING	    While the Kafka service hosts multiple topics, only those records from the clickstream topic are included here
value	    BINARY	    This is the full data payload (to be discussed later), sent as JSON
"""


# create table events_json from kafka-events.json file
spark.sql(
    """
CREATE TABLE IF NOT EXISTS events_json
    (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)
USING JSON
LOCATION "/Users/benkaan/Desktop/data-sparkling/databricks/data/kafka-events.json"
"""
)

spark.sql("SELECT COUNT(*) FROM events_json").show()
"""
+--------+
|count(1)|
+--------+
|    2570|
+--------+
"""
