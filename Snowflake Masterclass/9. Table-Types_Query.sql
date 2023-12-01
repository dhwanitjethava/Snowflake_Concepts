-- Permanent Table : Time-travel retention period 0 to 90 days; Fail-safe
-- (for permanent data)

-- Transient Table : Time-travel retention period 0 to 1 day;
-- (only for the data that does not need to be protected)

-- Temporary Table : Time-travel retention period 0 to 1 day; - exist only in the current session
-- (only for non-permanent data)

-- Types are also available for other database objects (database, schema, etc.)
-- For temporary table no naming conflicts with permanent/transient tables.
-- - Other tables will be effectively hidden!

-----------------------------------------------------------------------------------------------------------------------

-- Permanent Tables

-- View table matrics (takes a bit to appear)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- View detailed table matrics (takes a bit to appear)
SELECT 	ID, 
       	TABLE_NAME, 
		TABLE_SCHEMA,
        TABLE_CATALOG,
		ACTIVE_BYTES / (1024*1024*1024) AS ACTIVE_STORAGE_USED_GB,
		TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB,
		FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_STORAGE_USED_GB,
        IS_TRANSIENT,
        DELETED,
        TABLE_CREATED,
        TABLE_DROPPED,
        TABLE_ENTERED_FAILSAFE
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
--WHERE TABLE_CATALOG ='PDB'
WHERE TABLE_DROPPED is not null
ORDER BY FAILSAFE_BYTES DESC;

-----------------------------------------------------------------------------------------------------------------------

-- Transient Tables

-- Create Transient table
CREATE OR REPLACE TRANSIENT TABLE <transient_table_name> (
    column1 int,
    column2 text,
    column3 number
);

-- View tables
SHOW TABLES;

-- View table matrics (takes a bit to appear)
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Set retention time to zero and drop the transient table -> No option to get table back
ALTER TABLE <transient_table_name>
SET DATA_RETENTION_TIME_IN_DAYS = 0;

DROP TABLE <transient_table_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Temporary Tables

-- Create Temporary table
CREATE OR REPLACE TEMPORARY TABLE <temporary_table_name> (
    column1 int,
    column2 text,
    column3 number
);

-- View tables
SHOW TABLES;

-----------------------------------------------------------------------------------------------------------------------