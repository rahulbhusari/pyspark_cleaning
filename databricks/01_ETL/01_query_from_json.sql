
-- read directly from json
SELECT * FROM json.`${databricks/data/kafka-events.json}`


-- create reference to json files in a dir
CREATE OR REPLACE VIEW events_view
AS SELECT * FROM json.`${databricks/data/jsondir}`


-- temp view exists only for the current SparkSession
CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${databricks/data/kafka-events.json}`


-- create Common Table Expression(CTE)
WITH cte_events
AS (SELECT * FROM json.`${databricks/data/kafka-events.json}`)
SELECT * FROM cte_events

-- extract text files as raw strings
SELECT * FROM text.`${databricks/data/kafka-events.csv}`

-- extract raw bytes and metadata of a file
SELECT * FROM binaryFile.`${databricks/data/imagedir}`
