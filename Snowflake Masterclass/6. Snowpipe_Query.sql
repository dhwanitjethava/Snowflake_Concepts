-- Create Table
CREATE OR REPLACE TABLE <table_name> (
    column1 INT,
    column2 STRING,
    column3 DATE,
    column4 NUMBER,
    );
    
-- Create File Format object
CREATE OR REPLACE FILE FORMAT <file_format_name>
    TYPE = CSV
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;

-- Create Stage with integration object & file format object
CREATE OR REPLACE stage <stage_object_name>
    URL = 's3_url'
    STORAGE_INTEGRATION = S3_INT
    FILE_FORMAT = <file_format_name>;

-- List Stage object
LIST @<stage_object_name>;

-- Define Snowpipe object
CREATE OR REPLACE PIPE <pipe_name>
auto_ingest = TRUE
AS
COPY INTO <table_name>
FROM @<stage_object_name>;

-- Describe Pipe
DESC PIPE <pipe_name>;
-- from description of the pipe, copy the notification_channel ARN and paste it into S3 event configuration - SQS Queue

-- Run below query to check if data ingest via Snowpipe or not.
-- Snowpipe works within 30 to 60 seconds to ingest data in Snowflake Database.
SELECT * FROM <table_name>;

-----------------------------------------------------------------------------------------------------------------------
-- Handling Errors

-- Pipe refresh command - results last filenames for SQS notification occured
ALTER PIPE <pipe_name> refresh

-- Validate Pipe is actually working - results JSON file showing pipe status
SELECT SYSTEM$PIPE_STATUS('<pipe_name>')

-- Snowpipe error message since last 2 hours
SELECT * FROM TABLE (VALIDATE_PIPE_LOAD(
    PIPE_NAME => '<pipe_name>',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));

-- COPY command history from table to see error massage
SELECT * FROM TABLE (INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME => '<table_name>',
    START_TIME => DATEADD(HOUR,-2,CURRENT_TIMESTAMP())));

-----------------------------------------------------------------------------------------------------------------------
-- Snowpipe Management

-- Describe Snowpipe
DESC PIPE <pipe_name>;

-- List of all Snowpipes
SHOW PIPES;

SHOW PIPES LIKE '%name%';

SHOW PIPES IN DATABASE <database_name>;

SHOW PIPES IN SCHEMA <schema_name>;

SHOW PIPES LIKE '%name%' IN DATABASE <database_name>;

-----------------------------------------------------------------------------------------------------------------------
-- Chnaging Pipe (alter stage or file format)

-- Pause Pipe
ALTER PIPE <pipe_name>
SET PIPE_EXECUTION_PAUSED = TRUE

-- Resume Pipe
ALTER PIPE <pipe_name>
SET PIPE_EXECUTION_PAUSED = FALSE

-- Recreate the Pipe to change COPY statement in the pipe definition
-- Note : Metadata of the Pipe is stored if alter or recreate the Pipe
CREATE OR REPLACE PIPE <pipe_name>
auto_ingest = TRUE
AS
COPY INTO <another_table_name>
FROM @<another_stage_object_name>;

-- Check metadata of the file after alter or recreate
ALTER PIPE <pipe_name> refresh

-----------------------------------------------------------------------------------------------------------------------
-- Note: Snowpipe for Azure is tricky as compared to AWS.