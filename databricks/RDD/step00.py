from pyspark.sql import SparkSession

# create a Spark Session object
spark = SparkSession.builder.master("local").getOrCreate()

# create a Spark Context object
sc = spark.sparkContext

# create RDD
dirty_sales_data_RDD = sc.textFile("data/dirty_sales_data.csv")

# save the first row as header
header = dirty_sales_data_RDD.first()

# filter out the header
filtered_sales_data_RDD = dirty_sales_data_RDD.filter(lambda x: x != header)

# print the first row
print(filtered_sales_data_RDD.first())  # John,1,$100,000,8/1/2022

# using comma delimiter, separate column values in each row
headerless_sales_data_RDD = filtered_sales_data_RDD.map(lambda x: x.split(","))

# combine thrid and forth rows
pretty_sales_data_RDD = headerless_sales_data_RDD.map(
    lambda x: [x[0], x[1], x[2] + x[3], x[4]]
)

# remove the $ from third column
pretty_sales_data_RDD = pretty_sales_data_RDD.map(
    lambda x: [x[0], x[1], x[2].strip("$"), x[3]]
)

print(pretty_sales_data_RDD.first())  # ['John', '1', '100000', '8/1/2022']
