from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local").getOrCreate()

dirty_sales_data_df = spark.read.csv(
    path="data/dirty_sales_data.csv", header=True, sep=","
)
dirty_sales_data_df.show()

"""
ISSUES => comma-seperated Sales_Amount column, $ sign, data types
+-------------+----------------+-------------+----------------+
|Employee_Name| Employee_Number| Sales_Amount| Sale_Close_Date|
+-------------+----------------+-------------+----------------+
|         John|               1|         $100|             000|
|      Melissa|               2|         $250|             000|
|       Edward|               3|          $50|             000|
|     Samantha|               4|         $200|             000|
|         Ryan|               5|          $30|             000|
|        Lizzy|               6|          $20|             000|
|         Jose|               7|         $500|             000|
|         Irma|               8|         $300|             000|
|         Paul|               9|         $350|             000|
|        Sandy|              10|         $200|             000|
|         John|              11|           $1|             000|
|      Melissa|              20|         $250|             000|
|       Edward|              30|         $500|             000|
|     Samantha|              40|          $20|             000|
|         Ryan|              50|         $300|             000|
|        Lizzy|              60|         $205|             000|
|         Jose|              70|          $50|             000|
|         Irma|              80|          $30|             000|
|         Paul|              90|          $50|             000|
|        Sandy|              29|         $220|             000|
+-------------+----------------+-------------+----------------+


"""
