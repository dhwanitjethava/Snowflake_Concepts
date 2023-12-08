-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id int,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
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
   first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Phone STRING,
    Job STRING
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

------------------------------------------------------------------------------------------------------------------------

---- UNDROP objects ----

-- Setting up table
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test (
    id int,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    Phone STRING
    );

-- Load data into table
COPY INTO OUR_FIRST_DB.public.test
    FROM @MANAGE_DB.external_stages.time_travel_stage
    FILES = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.test;

-- USE-CASE : Update data (by mistake)

UPDATE OUR_FIRST_DB.public.test
    SET LAST_NAME = 'Tyson';

UPDATE OUR_FIRST_DB.public.test
    SET JOB = 'Data Analyst';

SELECT * FROM OUR_FIRST_DB.public.test BEFORE (STATEMENT => 'query_id');

-- Restoring (Good method)
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.test_backup AS
SELECT * FROM OUR_FIRST_DB.public.test BEFORE (STATEMENT => '01ab3a59-0001-079e-0003-a4c60002d0ee');

TRUNCATE OUR_FIRST_DB.public.test;

INSERT INTO OUR_FIRST_DB.public.test
SELECT * FROM OUR_FIRST_DB.public.test_backup;

SELECT * FROM OUR_FIRST_DB.public.test ;

-- Undrop preparation

-- Setting up table
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.time_travel_stage
    URL = 's3://data-snowflake-fundamentals/time-travel/'
    FILE_FORMAT = MANAGE_DB.file_formats.csv_file;
    
CREATE OR REPLACE TABLE OUR_FIRST_DB.public.customers (
    id int,
    first_name STRING,
    last_name STRING,
    email STRING,
    gender STRING,
    Job STRING,
    phone STRING
    );

COPY INTO OUR_FIRST_DB.public.customers
    FROM @MANAGE_DB.external_stages.time_travel_stage
    EXTERNAL_TABLE_FILES = ('customers.csv');

SELECT * FROM OUR_FIRST_DB.public.customers;

-- Undrop command - Tables
DROP TABLE OUR_FIRST_DB.public.customers;

SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP TABLE OUR_FIRST_DB.public.customers;

-- UNDROP command - Schemas
DROP SCHEMA OUR_FIRST_DB.public;

SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP SCHEMA OUR_FIRST_DB.public;

SELECT * FROM customers;

-- UNDROP command - Database

DROP DATABASE OUR_FIRST_DB;

SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP DATABASE OUR_FIRST_DB;

-- Undroping a with a name that already exists
SELECT * FROM OUR_FIRST_DB.public.customers;

UNDROP table OUR_FIRST_DB.public.customers;

ALTER TABLE OUR_FIRST_DB.public.customers
RENAME TO OUR_FIRST_DB.public.customers_new;
