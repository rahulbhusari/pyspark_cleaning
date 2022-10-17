-- direct query from a text file
SELECT * FROM csv.`${databricks/data/sales.csv}`

/*

_c0
order_id|email|transactions_timestamp|total_item_quantity|purchase_revenue_in_usd|unique_items|items
298592|sandovalaustin@holder.com|1592629288475307|1|850.5|1|[{'coupon': 'NEWBED10'
299024|msmith@monroe.com|1592636869915092|2|1092.6|2|[{'coupon': 'NEWBED10'
300048|robertstimothy@hotmail.com|1592649862529478|1|1075.5|1|[{'coupon': 'NEWBED10'
298711|lovejamie@yahoo.com|1592631406799948|1|850.5|1|[{'coupon': 'NEWBED10'
301760|jennifer7054@gmail.com|1592661071882666|1|940.5|1|[{'coupon': 'NEWBED10'

*/

-- no data is moved during table creation.
CREATE TABLE IF NOT EXISTS sales_csv
    (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
    header = "true"
    delimiter = "|"
)
LOCATION "${databricks/data/sales.csv}"


-- we are just pointing to the cs files stored in an external location
SELECT * FROM sales_csv


/*
+--------+--------------------+----------------------+-------------------+-----------------------+------------+--------------------+
|order_id|               email|transactions_timestamp|total_item_quantity|purchase_revenue_in_usd|unique_items|               items|
+--------+--------------------+----------------------+-------------------+-----------------------+------------+--------------------+
|  298592|sandovalaustin@ho...|      1592629288475307|                  1|                  850.5|           1|[{'coupon': 'NEWB...|
|  299024|   msmith@monroe.com|      1592636869915092|                  2|                 1092.6|           2|[{'coupon': 'NEWB...|
|  300048|robertstimothy@ho...|      1592649862529478|                  1|                 1075.5|           1|[{'coupon': 'NEWB...|
+--------+--------------------+----------------------+-------------------+-----------------------+------------+--------------------+
*/

-- spark will load columns in the order specified in the table creation
DESCRIBE EXTENDED sales_csv

-- Because Spark automatically cashed the underlying data in local storage
-- after we append more data to the existing sales_csv table
-- when we run subsequent queires, Spark will use the local cache to provide the optimal performance
-- For this reason, we may need to refresh the table
-- DELTA LAKE guarantees that this referesh is not needed because you ALWAYS query the most recent version of your source data


REFRESH TABLE sales_csv