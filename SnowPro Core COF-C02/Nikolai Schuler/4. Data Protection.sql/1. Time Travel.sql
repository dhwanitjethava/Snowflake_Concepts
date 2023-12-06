-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id int,
    first_name string,
    last_name string,
    email string,
    gender string,
    Job string,
    Phone string
    );

-- Create file format
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_file
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- Create stage
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    FILE_FORMAT = MANAGE_DB.file_formats.csv_file;

LIST @MANAGE_DB.external_stages.time_travel_stage;

-- Load data into table
COPY INTO OUR_FIRST_DB.public.test
    FROM @MANAGE_DB.external_stages.time_travel_stage
    FILES = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.test;

---- USE-CASE : Update data (by mistake) ----

UPDATE OUR_FIRST_DB.public.test
SET FIRST_NAME = 'Joyen' ;

-- Method 1 - 3 minutes back
SELECT * FROM OUR_FIRST_DB.public.test at (OFFSET => -60*3);

-- Method 2 - before timestamp
SELECT * FROM OUR_FIRST_DB.public.test before (timestamp => 'timestamp'::timestamp);

SELECT CURRENT_TIMESTAMP;

-- Setting up UTC time for convenience
ALTER SESSION SET TIMEZONE ='America/Los_Angeles';
SELECT CURRENT_TIMESTAMP;

-- Method 3 - before Query ID

-- Preparing table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
   id int,
   first_name string,
    last_name string,
    email string,
    gender string,
    Phone string,
    Job string
    );

-- Load data into table
COPY INTO OUR_FIRST_DB.public.test
    FROM @MANAGE_DB.external_stages.time_travel_stage
    FILES = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.test;

-- Altering table (by mistake)
UPDATE OUR_FIRST_DB.public.test
    SET EMAIL = NULL;

SELECT * FROM OUR_FIRST_DB.public.test;

SELECT * FROM OUR_FIRST_DB.public.test BEFORE (statement => 'query_id');
