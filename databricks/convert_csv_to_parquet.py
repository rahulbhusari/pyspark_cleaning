from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[*]").getOrCreate()

parquetDF = spark.read.csv(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete.csv"
)
parquetDF.coalesce(1).write.mode("overwrite").parquet(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete-spark.parquet"
)

# PANDAS handle the conversion better and the [array] format reamins intact unlike spark output from above
import pandas as pd

pandasDF = pd.read_csv(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete.csv"
)
pandasDF.to_parquet(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete-pandas.parquet"
)


# PYARROW offers the fastest performance
from pyarrow import csv, parquet

table = csv.read_csv(
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete.csv"
)
parquet.write_table(
    table,
    "/Users/benkaan/Desktop/data-sparkling/databricks/data/sales-complete-pyarrow.parquet",
)
