-- Use Snowflake Sample Database
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER WAREHOUSE COMPUTE_WH RESUME;

USE ROLE ACCOUNTADMIN;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCDS_SF1000TCL;

SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, CLUSTERING_KEY 
FROM INFORMATION_SCHEMA.TABLES
WHERE CLUSTERING_KEY IS NOT NULL AND TABLE_SCHEMA = 'TPCDS_SF100TCL';

---- Comparing different queries ----

-- Includes both cluster keys
SELECT * FROM WEB_SALES
WHERE WS_SOLD_DATE_SK = 2450859 AND WS_ITEM_SK IN (286952,286954);

-- Includes one cluster key
SELECT * FROM WEB_SALES
WHERE WS_SOLD_DATE_SK = 2450849 AND WS_BILL_HDEMO_SK IN (763, 755);

-- Includes no cluster key
SELECT * FROM WEB_SALES
WHERE WS_ITEM_SK =255608 AND WS_BILL_HDEMO_SK IN (763, 755);

---- How well table is clustered? ----

-- If table is clustered
SELECT SYSTEM$CLUSTERING_INFORMATION ('WEB_SALES');

-- Properties on specific columns
SELECT SYSTEM$CLUSTERING_INFORMATION ('WEB_SALES','(WS_SOLD_DATE_SK)');
SELECT SYSTEM$CLUSTERING_INFORMATION ('WEB_SALES','(WS_BILL_HDEMO_SK)');

SELECT SYSTEM$CLUSTERING_DEPTH ('WEB_SALES');

ALTER TABLE <table_name> ADD SEARCH OPTIMIZATION ON SUBSTRING(<column>);