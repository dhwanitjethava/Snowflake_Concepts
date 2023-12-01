-- Methods for Loading Data into Snowflake (No costing for loading data)
-- 1. Batch/Bulk loading
--    - Most frequent method
--    - Uses warehouses
--    - Loading from stages
--    - COPY command
--    - Transformations possible
-- 2. Continuous loading
--    - Designed to load small volumes of data
--    - Automatically once they are added to stages
--    - Lates results for analysis
--    - Snowpipe (Serverless feature)

-----------------------------------------------------------------------------------------------------------------------

-- Stages: Location of data files where data can be loaded from (not confused with data warehouse stages).
-- External Stage
-- - External cloud provider (AWS, GCP, Azure)
-- - Stage object created in the schema
-- - Additional costs may apply if region/platform differs

-----------------------------------------------------------------------------------------------------------------------

-- Creating external stage

CREATE OR REPLACE STAGE <external_stage_name>
    URL = '<url>'
    CREDENTIALS = (aws_key_id = 'ABCD_DUMMY_ID' aws_secret_key = '1234abcd_key');

-- Description of external stage
DESC STAGE <external_stage_name>;

-- Alter external stage
ALTER STAGE <external_stage_name>
SET CREDENTIALS = (aws_key_id = 'XYZ_DUMMY_ID' aws_secret_key = '987xyz');

-- Publicly accessible staging area    
CREATE OR REPLACE STAGE <external_stage_name>
    URL = '<url>';

-- List files in the stage
LIST @aws_stage;

-- Load data using COPY command
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>');

COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    PATTERN = '.*<name>.*csv';      -- wildcard character - '.* string .*'

-- When files already loaded into Snowflake, then it will not loaded again from COPY command.

-----------------------------------------------------------------------------------------------------------------------

-- Transformation of data

-- Transforming data using the SELECT statement
COPY INTO <table_name>
    FROM (SELECT es.$1, es.$2, es.$3 FROM @<external_stage_name> es)
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>');

-- COPY Command using a SQL function (subset of functions available)
COPY INTO <table_name>
    FROM (SELECT 
            es.$1,
            es.$2, 
            es.$3,
            CASE WHEN CAST(es.$3 AS INT) < 0 THEN '<string>' ELSE '<string>' END 
          FROM @<external_stage_name> es)
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>');

-- COPY Command using a SQL function (subset of functions available)
COPY INTO <table_name>
    FROM (SELECT 
            es.$1,
            es.$2, 
            es.$3,
            SUBSTRING(es.$5, 1, 5) 
          FROM @<external_stage_name> es)
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>');

-- COPY Command using subset of columns
COPY INTO <table_name> (column1, column2)
    FROM (SELECT 
            es.$1,
            es.$3
          FROM @<external_stage_name> es)
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name>');

-- Table Autoincrement
CREATE OR REPLACE TABLE <table_name> (
    ID NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    column2 <datatype>,
    column3 <datatype>
    );

COPY INTO <table_name> (column2, column3)
    FROM (SELECT 
            es.$2,
            es.$3
          FROM @<external_stage_name> es)
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name>');

-----------------------------------------------------------------------------------------------------------------------

-- COPY statement option ON_ERROR

-- Error handling using the ON_ERROR = 'CONTINUE'
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name>')
    ON_ERROR = 'CONTINUE';

-- Error handling using the ON_ERROR = ABORT_STATEMENT (default)
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name>')
    ON_ERROR = 'ABORT_STATEMENT';

-- Error handling using the ON_ERROR = SKIP_FILE
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>')
    ON_ERROR = 'SKIP_FILE';  -- skip file if any errors_seen on the file

-- Error handling using the ON_ERROR = SKIP_FILE_<number> (number = error_limit)
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>')
    ON_ERROR = 'SKIP_FILE_2';  -- skip file if errors_seen >= error_limit

-- Error handling using the ON_ERROR = SKIP_FILE_<number%> (% amount is the total number of rows in the file or table)
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>')
    ON_ERROR = 'SKIP_FILE_3%';  -- skip file if error_seen% >= error_limit%

-- Error handling using the SIZE_LIMIT = <number> (number = size of individual file in MB)
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>')  -- file_name1 should load first and then file_name2 loaded.
    ON_ERROR = SKIP_FILE_3  -- skip file if errors_seen >= error_limit
    SIZE_LIMIT = 30;  -- skip file if size of files (together) per COPY query > 30 Bytes
-- Note: At least one file is loaded regardless of the SIZE_LIMIT unless there is no file to be loaded.

-----------------------------------------------------------------------------------------------------------------------

-- FILE FORMAT

-- Create file-format object (object created in schema)
CREATE OR REPLACE FILE FORMAT <file_format_name>
    TYPE = CSV | JSON | PARQUET | AVRO | XML | ORC    -- default TYPE = CSV
    FIELD_DELIMITER = ',' | ';' | '|'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- See properties of file format object
DESC FILE FORMAT <file_format_name>;

-- Using FILE FORMAT object in COPY command      
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name>)
    FILES = ('<file_name1>', '<file_name2>');

-- Altering FILE FORMAT object
ALTER FILE FORMAT <file_format_name>
    SET SKIP_HEADER = 1;

-- Altering the TYPE of a FILE FORMAT object is not possible
ALTER FILE FORMAT <file_format_name>
    SET TYPE = CSV;  -- gives you error

-- Overwriting properties of FILE FORMAT object      
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = (FORMAT_NAME = <file_format_name> FIELD_DELIMITER = ';' SKIP_HEADER = 1)
    FILES = ('<file_name1>', '<file_name2>')
    ON_ERROR = SKIP_FILE_3;
-- In overwriting properties of FILE FORMAT object, it only applicable in the above command
-- If we see the properties of the FILE FORMAT, it does not change by overwriting on COPY command

-----------------------------------------------------------------------------------------------------------------------

-- Create storage integration object
CREATE OR REPLACE STORAGE INTEGRATION s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = ''
    STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket-name>/<your-path>/', 's3://<your-bucket-name>/<your-path>/')
    COMMENT = 'This an optional comment';
   
   
-- See storage integration properties to fetch external_id so we can update it in S3
DESC INTEGRATION s3_int;

-----------------------------------------------------------------------------------------------------------------------