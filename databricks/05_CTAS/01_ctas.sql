/*
    Create Table As Select (CTAS) statements do not support:
        - manual schema declaration:
            they are useful for data ingestions from sources with well-defined schemas like parquet or table.
        - specifying additional file option
            this is a big issue with csv file sources
*/

-- no issue since the parquet file has well-defined schema
CREATE OR REPLACE TABLE sales
AS
SELECT * FROM parquet.`{databricks/data/sales-complete-pandas.parquet}`;

DESCRIBE EXTENDED sales;

-- issue with this
CREATE OR REPLACE TABLE sales_csv
AS
SELECT * FROM csv.`{databricks/data/sales-complete.csv}`;

-- SOLUTION -> create a temp view first and build the CTAS from the temp view
CREATE OR REPLACE TEMP VIEW sales_temp_view
    (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS(
    path="${databricks/data/sales-complete-pandas.parquet}",
    header="true",
    delimiter="|"
);

CREAT TABLE sales_delta
AS
SELECT * FROM sales_temp_view;

SELECT * FROM sales_delta;


-- create a purchases table by filtering sales table created from parquet file
--------------- method 1 --------------------------------
CREATE OR REPLACE TABLE purchases
AS
SELECT order_id AS id, transactions_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases;


---------------- method 2 --------------------------------
CREATE OR REPLACE VIEW purchases_view
AS
SELECT order_id AS id, transactions_timestamp, purchase_revenue_in_usd AS price
FROM sales;

SELECT * FROM purchases_view;


