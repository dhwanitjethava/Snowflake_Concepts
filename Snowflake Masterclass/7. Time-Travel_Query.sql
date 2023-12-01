-- Time-travel retention period for Standard edition - upto 1 day
-- Time-travel retention period for other editions - upto 90 days
-- Default value of retention period for all editions - 1 day
-----------------------------------------------------------------------------------------------------------------------

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
    skip_header = 1;

-- Create Stage with integration object & file format object
CREATE OR REPLACE stage <stage_object_name>
    URL = 's3_url'
    FILE_FORMAT = <file_format_name>;

-- List files in Stage
LIST @<stage_object_name>;

-- COPY INTO Tbale from Stage
COPY INTO <table_name>
FROM @<stage_object_name>
FILES = ('file_name_present_at_stage');

-- Verify data in the table
SELECT * FROM <table_name>

-----------------------------------------------------------------------------------------------------------------------

-- Use-case 1: Update data (by mistake)
UPDATE <table_name>
SET FIRST_NAME = 'Dhwanit';

-- Selects historical data from a table as of 5 minutes ago:
SELECT * FROM <table_name> AT(OFFSET => -60*5); -- OFFSET => -(time in seconds)

-- Selects historical data from a table as before specified timestamp:
SELECT * FROM <table_name> BEFORE(TIMESTAMP => '2023-12-07 16:20:00.007'::timestamp_tz); 

-- Selects historical data from a table as of the date and time represented by the specified timestamp:
SELECT * FROM <table_name> AT(TIMESTAMP => 'Fri, 01 May 2023 16:20:00 -0700'::timestamp_tz); 

-- Selects historical data from a table up to, but not including any changes made by the specified statement:
SELECT * FROM <table_name> BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

-----------------------------------------------------------------------------------------------------------------------

-- Restoring Data

-- Bad method - If we recreate the table, we loose metadata regarding old version of that table
CREATE OR REPLACE TABLE <table_name> AS
SELECT * FROM <table_name> BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

-- Good method - Create another/backup table and then truncate original table and insert data into original table
CREATE OR REPLACE TABLE <another_table_name> AS
SELECT * FROM <table_name> BEFORE(STATEMENT => '8e5d0ca9-005e-44e6-b858-a8f5b37c5726');

TRUNCATE <table_name>;

INSERT INTO <table_name>
SELECT * FROM <another_table_name>;

SELECT * FROM <table_name>; -- Original table before update query

-----------------------------------------------------------------------------------------------------------------------

-- UNDROP Table command
DROP TABLE <table_name>;
UNDROP TABLE <table_name>;

-- UNDROP Schema command
DROP SCHEMA <schema_name>;
UNDROP SCHEMA <schema_name>;

-- UNDROP Database command
DROP DATABASE <database_name>;
UNDROP DATABASE <database_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Undroping with a table that already exists

-- Firstly we created one table and then made two mistakes one by one. 
-- Then we run the below query to only correct the second mistake.
CREATE OR REPLACE TABLE <table_name> AS
SELECT * FROM <table_name> BEFORE(STATEMENT => '<second_mistake_query_id>');

-- If we run SELECT statement on the table -> Table has first mistake
SELECT * FROM <table_name>; 
-- So now if you run 1st query to correct first mistake, You got an error due to that table is replaced (i.e. droped)

-- Now, Rename the existing table
ALTER TABLE <table_name>
RENAME TO <other_table_name>;

-- Now, undrop the main table -> then you can use time-travel feature without error
SELECT * FROM <table_name> BEFORE(STATEMENT => '<first_mistake_query_id>');

-----------------------------------------------------------------------------------------------------------------------

-- Retention Time

-- To know about Retention time
SHOW TABLES LIKE '%table%';

-- Method 1 : Change Retention time
ALTER TABLE <table_name>
SET DATA_RETENTION_TIME_IN_DAYS = 3;

-- Method 2 : Change Retention time
CREATE OR REPLACE TABLE <table_name> (
    column1 INT,
    column2 TEXT,
    column3 NUMBER
    )
    DATA_RETENTION_TIME_IN_DAYS = 7;

-- If we set Retention time 0 then we can't use time-travel feature
ALTER TABLE <table_name>
SET DATA_RETENTION_TIME_IN_DAYS = 0;    -- also can not use UNDROP feature

-----------------------------------------------------------------------------------------------------------------------

-- Time-travel Cost

-- More the time-travel retention period, more will be storage cost

-- Information regarding storage cost
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;

-- More information regarding storage cost
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Query time-travel storage
SELECT  ID, 
		TABLE_NAME, 
		TABLE_SCHEMA,
        TABLE_CATALOG,
		ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
		TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY STORAGE_USED_GB DESC,TIME_TRAVEL_STORAGE_USED_GB DESC;

-----------------------------------------------------------------------------------------------------------------------