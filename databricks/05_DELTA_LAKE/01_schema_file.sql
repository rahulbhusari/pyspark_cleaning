-- Default schema location is -> dbfs:/user/hive/warehouse/<schema_name>.db
CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_default_location;

-- We can also specify the location
CREATE SCHEMA IF NOT EXISTS ${da.schema_name}_custom_location
LOCATION '${da.paths.working_dir}/${da.schema_name}_custom_location.db';

-- Can verify/identify the location with
DESCRIBE SCHEMA EXTENDED ${da.schema_name}_default_location;

-- Create a managed table
USE ${da.schema_name}_default_location;

CREATE OR REPLACE TABLE managed_table_in_db_with_default_location
    (first_name STRING, last_name STRING, age INT, zipcode INT)
INSERT INTO managed_table_in_db_with_default_location
VALUES('John', 'Doe', 37, 01110);

SELECT * FROM managed_table_in_db_with_default_location;

/*

When we drop a managed table its log and data files are deleted.
Only the schema directory remains.

When we drop an external table, in the example below, the table definition no longer exits.
However, the underlying data remains intact.


*/
-- Create an external(unmanaged) table from a local CSV file
USE ${da.schema_name}_default_location;

CREATE OR REPLACE TEMPORARY VIEW temp_delays
USING CSV
OPTIONS(
    path = "${da.paths.datasets}/flights/departuredelays.csv",
    header = "true",
    mode = "FAILFAST" -- abort file parsing with a RuntimeException if any malformed lines are encountered
);
CREATE OR REPLACE TABLE external_table
LOCATION "${da.paths.working_dir}/external_table"
AS
    SELECT * FROM temp_delays;

SELECT * FROM external_table;