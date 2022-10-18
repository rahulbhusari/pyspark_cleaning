-- we can leverage built-in Spark SQL functions such as:
--  current_timestamp() -> records the timestamp when the logic is executed
--  input_file_location() -> records the source data file for each record in the table

CREATE OR REPLACE TABLE users_pii
COMMENT "Contains PII"
LOCATION "${da.paths.working_dir}/temp/users_pii"
PARTITIONED BY (first_touch_date)
AS
    SELECT *,
    CAST(CAST(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) first_touch_date,
    current_timestamp() updated_date,
    input_file_name() source_file,
    FROM parquet. `${source file location}`;

SELECT * FROM users_pii;

/*
Because data is partitioned by date, Delta Lake engine will create seperate files based on each date.
This can lead to the issue of having too many small files.
For this reason, partitioning is beneficial for medium and large size files.

*/