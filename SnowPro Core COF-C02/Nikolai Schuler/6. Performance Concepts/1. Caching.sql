USE ROLE ACCOUNTADMIN;
USE SCHEMA SNOWFLAKE_SAMPLE_DATA.TPCH_SF10;

---- Query Result Cache ----

-- Use of Cache
SELECT * FROM ORDERS;

ALTER WAREHOUSE COMPUTE_WH SUSPEND;
ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = FALSE;

ALTER WAREHOUSE COMPUTE_WH RESUME;

-- When Result Cache is not used

-- 1. Different syntax
SELECT * FROM ORDERS;
SELECT * FROM ORDERS LIMIT 4;

-- 2. Context functions are included
SELECT current_time(), * FROM ORDERS LIMIT 4;

-- 3. UDFs or external functions are included
SELECT MANAGE_DB.PUBLIC.ADD_TWO(3), * FROM ORDERS LIMIT 4;

-- 4. Missing privileges or USED_CACHED_RESULTS is set to false
ALTER SESSION SET USE_CACHED_RESULT = FALSE;

---- Metadata-based Cached ----

ALTER WAREHOUSE COMPUTE_WH SET AUTO_RESUME = FALSE;
ALTER WAREHOUSE COMPUTE_WH SUSPEND;

-- 1. Statistics about table objects
SELECT COUNT(*) FROM ORDERS;
SELECT MAX(O_TOTALPRICE) FROM ORDERS;
SELECT MAX(O_ORDERKEY) FROM ORDERS;

-- 2. Object metadata
SHOW ROLES;
DESC TABLE ORDERS;

-- 3. System-functions & context functions
SELECT SYSTEM$TYPEOF('a');
SELECT CURRENT_DATE();

-- 4. Local warehouse Cache
ALTER WAREHOUSE COMPUTE_WH RESUME;
SELECT * FROM ORDERS WHERE O_ORDERKEY < 3000000 LIMIT 5000;
