-- declare schema using generated columns
-- here we are converting the unix time in the original data to YYYY/MM/dd date format
-- any attempt to insert manual data such as ('2020-01-01') will fail simply because
-- the data must match the logic used in the GENERATED COLUMN

CREATE OR REPLACE TABLE purchase_dates (
    id STRING,
    transaction_timestamp STRING,
    price STRING,
    date DATE GENERATED ALWAYS AS(
        cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE)
        COMMENT "generated based on 'transaction timestamp' column"
    )
)

-- enable automerge feature
SET spark.databricks.delta.schema.automerge.enabled=true;

MERGE INTO purchase_dates pd
USING purchases p
ON pd.id = p.id
WHEN NOT MATCHED THEN
    INSERT *

/*
id      transaction_timestamp   price   date
257436  1592193956703494        2190.0  2020-06-15
257452  1592201856856023        1195.0  2020-06-15
*/

-- we can create a check constraint on the date column
-- this will reflect in the TBLPROPERTIES field of 'DESCRIBE EXTENDED purchase_dates' command
ALTER TABLE purchase_dates ADD CONSTRAINT valid_date_constraint CHECK (date > '2020-01-01');