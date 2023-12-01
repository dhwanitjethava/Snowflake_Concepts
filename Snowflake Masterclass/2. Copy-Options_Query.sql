---- VALIDATION MODE ----

-- Load data using COPY command
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv'
    VALIDATION_MODE = RETURN_ERRORS | RETURN_n_ROWS;
-- VALIDATION_MODE - Validate the data files instead of loading them.
-- RETURN_ERRORS - Returns all errors in COPY command
-- RETURN_n_ROWS - Validates & returns the specified number of rows; fails at the first error encountered
-- Data will not be loaded into table; rather it just returns in the query result

-----------------------------------------------------------------------------------------------------------------------

-- Working with rejected records

-- 1. Saving rejected files after VALIDATION_MODE

-- Rejected records from VALIDATION_MODE query result
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv'
    VALIDATION_MODE = RETURN_ERRORS;

-- Storing rejected /failed results in a table
CREATE OR REPLACE TABLE <rejected_table_name> AS
SELECT REJECTED_RECORD FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

CREATE OR REPLACE TABLE <rejected_table_name> AS
SELECT REJECTED_RECORD FROM TABLE(RESULT_SCAN('<query_id>'));

-- Adding additional records in table <rejected_table_name>
INSERT INTO <rejected_table_name>
SELECT REJECTED_RECORD FROM TABLE(RESULT_SCAN('<query_id>'));

-- SELECT query to show rejected_record
SELECT * FROM <rejected_table_name>;

-- 2. Saving rejected files without VALIDATION_MODE 

-- Load data with ON_ERROR = CONTINUE option
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv'
    ON_ERROR = CONTINUE;

-- See rejected_record after load data from COPY command
SELECT * FROM TABLE(VALIDATE(<table_name>, JOB_ID => '_LAST'));  -- last query executed

SELECT * FROM TABLE(VALIDATE(<table_name>, JOB_ID => '<query_id>'));  -- specify query_id

-- 3. Working with rejected records

-- SELECT REJECTED_RECORD from rejected_table
SELECT REJECTED_RECORD FROM <rejected_table_name>;

-- Create another table for inserting rejected_values from rejected_table
CREATE OR REPLACE TABLE <rejected_values_table_name> AS
SELECT
    SPLIT_PART(rejected_record,',',1) as <column1>, 
    SPLIT_PART(rejected_record,',',2) as <column2>, 
    SPLIT_PART(rejected_record,',',3) as <column3>, 
    SPLIT_PART(rejected_record,',',4) as <column4>
FROM <rejected_table_name>;

SELECT * FROM <rejected_values_table_name>;
    
-----------------------------------------------------------------------------------------------------------------------

---- RETURN_FAILED_ONLY = TRUE | FALSE (default) ----

-- Load data using COPY command
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv'
    ON_ERROR = CONTINUE
    RETURN_FAILED_ONLY = TRUE;  -- only return failed to load files OR partially loaded files
-- Specifies whether to return only files that have failed to load in the statement result

-----------------------------------------------------------------------------------------------------------------------

---- TRUNCATECOLUMNS = TRUE | FALSE (default) ----

-- Load data using COPY command
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv'
    TRUNCATECOLUMNS = TRUE;
-- If we specifies character length while creating table, then if some value of column while loading data exceeded,
-- then it will truncate the value to the limit.
--     example: category varchar(10) then, 'Electronics' -> 'Electronic'
-- Specifies whether to truncate text strings that exceed the target column length
-- TRUE = strings are automatically truncated to the target column length
-- FALSE = COPY produces an error if a loaded string exceeds the target column length

-----------------------------------------------------------------------------------------------------------------------

---- FORCE = TRUE | FALSE (default) ----

-- Load data using COPY command
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv'
    FORCE = TRUE;
-- Specifies to load all files, regardless of whether they've been loaded previously and have not changed since they
--  were loaded.
-- Note: This option reloads files, potentially duplicating data in a table.

-----------------------------------------------------------------------------------------------------------------------

---- Load History ----
-- Enables you to retrieve the history of data loaded into tables using COPY command

-- Query load history within a database
SELECT * FROM <database_name>.INFORMATION_SCHEMA.LOAD_HISTORY;

-- Query load history globally from SNOWFLAKE database
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY;

-- Filter on specific table & schema
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
    WHERE SCHEMA_NAME = '<schema_name>' AND
    TABLE_NAME = '<table_name>' AND
    ERROR_COUNT > 0;  -- result specified error counts

-- Filter for data loaded yesterday to any date of loading
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
    WHERE DATE(LAST_LOAD_TIME) <= DATEADD(DAYS, -1, CURRENT_DATE);

-----------------------------------------------------------------------------------------------------------------------