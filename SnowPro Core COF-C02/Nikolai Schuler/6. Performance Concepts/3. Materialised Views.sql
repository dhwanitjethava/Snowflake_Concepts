-- Create database
CREATE DATABASE MV_DB;

-- Our sample query
SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
    WHERE C_NATIONKEY = 7 AND C_MKTSEGMENT LIKE 'MACHINERY' AND C_COMMENT LIKE '%even%';

-- Create materialized view
CREATE OR REPLACE MATERIALIZED VIEW  mv_db.public.v_1 AS 
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER 
    WHERE C_NATIONKEY = 7 AND C_MKTSEGMENT LIKE 'MACHINERY' AND C_COMMENT LIKE '%even%';

-- Don't use cache and for test materialized view
ALTER SESSION SET USE_CACHED_RESULT = FALSE;
ALTER WAREHOUSE  COMPUTE_WH SUSPEND;
ALTER WAREHOUSE  COMPUTE_WH RESUME;

-- Select on materialized view
SELECT * FROM mv_db.public.v_1; 

-- View refresh history from information_schema
SELECT * FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY());

SELECT * FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY(MATERIALIZED_VIEW_NAME => 'mv_db.public.v_1'));

SELECT TO_DATE(start_time) AS date,
    database_name,
    schema_name,
    table_name,
    SUM(credits_used) AS credits_used
FROM snowflake.account_usage.materialized_view_refresh_history
WHERE start_time >= DATEADD(month,-1,CURRENT_TIMESTAMP())
GROUP BY 1,2,3,4
ORDER BY 5 DESC;

---- Updating the base table effects ----   

-- 1. Create sample table
CREATE OR REPLACE TABLE  mv_db.public.t_1 AS 
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER 
    WHERE C_NATIONKEY = 7 AND C_MKTSEGMENT LIKE 'MACHINERY' and C_COMMENT LIKE '%even%';

-- 2. Create materialized view from table
CREATE OR REPLACE MATERIALIZED VIEW  mv_db.public.v_2 AS 
    SELECT * FROM mv_db.public.t_1;

-- Select on materialized view
SELECT * FROM mv_db.public.v_2;

-- 3. Update base table
DELETE FROM mv_db.public.t_1 WHERE C_ADDRESS LIKE '%8%';

-- View materialized views information
SELECT * FROM MV_DB.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA ='PUBLIC';

SHOW MATERIALIZED VIEWS;

-- Drop database again
DROP DATABASE MV_DB;