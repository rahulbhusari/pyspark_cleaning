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
# print(filtered_sales_data_RDD.first())
# John,1,$100,000,8/1/2022

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

# print the first row
# print(pretty_sales_data_RDD.first())
# # ['John', '1', '100000', '8/1/2022']

# replace / with -
pretty_sales_data_RDD = pretty_sales_data_RDD.map(
    lambda x: [x[0], x[1], x[2], x[3].replace("/", "-")]
)

pretty_sales_data_RDD.first()  # ['John', '1', '100000', '8-1-2022']

# convert RDD to list
sales_data_list = pretty_sales_data_RDD.map(list)


# display the first 3 rows in the new list
# for item in sales_data_list.collect()[:3]:
#     print(item)
"""
['John', '1', '100000', '8-1-2022']
['Melissa', '2', '250000', '8-1-2022']
['Edward', '3', '50000', '8-2-2022']

"""

# convert the second and third row to integer and forth row to date
from datetime import datetime

clean_list = []
for row in sales_data_list.collect():
    emp_name = row[0]
    emp_num = int(row[1])
    sale_amt = int(row[2])
    sale_date = datetime.strptime(row[3], "%m-%d-%Y").strftime("%m-%d-%Y")

    row[0] = emp_name
    row[1] = emp_num
    row[2] = sale_amt
    row[3] = sale_date
    clean_list.append(row)

# print the first 3 rows
# for row in clean_list[:2]:
#     print(row)

"""
['John', 1, 100000, '08-01-2022']
['Melissa', 2, 250000, '08-01-2022']
"""


# create an employee dictionary
employee_dict = {
    "John": 1,
    "Melissa": 2,
    "Edward": 3,
    "Samantha": 4,
    "Ryan": 5,
    "Lizzy": 6,
    "Jose": 7,
    "Irma": 8,
    "Paul": 9,
    "Sandy": 10,
}

# update the employee id number based on the employee dictionary values
updated_list = []
for row in clean_list:
    name = row[0]
    emp_num = row[1]

    if name in employee_dict and emp_num == employee_dict[name]:
        updated_list.append(row)
    elif name in employee_dict and emp_num != employee_dict[name]:
        row[1] = employee_dict[name]
        updated_list.append(row)

# print(len(updated_list))
# print the first 20 rows
# for row in updated_list[:20]:
#     print(row)
"""
['John', 1, 100000, '08-01-2022']
['Melissa', 2, 250000, '08-01-2022']
['Edward', 3, 50000, '08-02-2022']
['Samantha', 4, 200000, '08-03-2022']
['Ryan', 5, 30000, '08-03-2022']
['Lizzy', 6, 20000, '08-03-2022']
['Jose', 7, 500000, '08-03-2022']
['Irma', 8, 300000, '08-04-2022']
['Paul', 9, 350000, '08-04-2022']
['Sandy', 10, 200000, '08-05-2022']
['John', 1, 100000, '08-01-2022']
['Melissa', 2, 250000, '08-01-2022']
['Edward', 3, 500000, '08-02-2022']
['Samantha', 4, 200000, '08-20-2022']
['Ryan', 5, 300000, '08-13-2022']
['Lizzy', 6, 205000, '08-23-2022']
['Jose', 7, 50000, '08-13-2022']
['Irma', 8, 30000, '08-14-2022']
['Paul', 9, 50000, '08-14-2022']

"""
# convert the list to a DF
cleaned_sales_data_DF = spark.createDataFrame(updated_list, header.split(","))


# create a temp view

cleaned_sales_data_DF.createTempView("sales")

query = """
SELECT *
FROM sales
"""
spark.sql(f"{query}").show()
"""
+-------------+----------------+-------------+----------------+
|Employee_Name| Employee_Number| Sales_Amount| Sale_Close_Date|
+-------------+----------------+-------------+----------------+
|         John|               1|       100000|      08-01-2022|
|      Melissa|               2|       250000|      08-01-2022|
|       Edward|               3|        50000|      08-02-2022|
|     Samantha|               4|       200000|      08-03-2022|
|         Ryan|               5|        30000|      08-03-2022|
|        Lizzy|               6|        20000|      08-03-2022|
|         Jose|               7|       500000|      08-03-2022|
|         Irma|               8|       300000|      08-04-2022|
|         Paul|               9|       350000|      08-04-2022|
|        Sandy|              10|       200000|      08-05-2022|
|         John|               1|       100000|      08-01-2022|
|      Melissa|               2|       250000|      08-01-2022|
|       Edward|               3|       500000|      08-02-2022|
|     Samantha|               4|       200000|      08-20-2022|
|         Ryan|               5|       300000|      08-13-2022|
|        Lizzy|               6|       205000|      08-23-2022|
|         Jose|               7|        50000|      08-13-2022|
|         Irma|               8|        30000|      08-14-2022|
|         Paul|               9|        50000|      08-14-2022|
|        Sandy|              10|       220000|      08-15-2022|
+-------------+----------------+-------------+----------------+
only showing top 20 rows

"""
