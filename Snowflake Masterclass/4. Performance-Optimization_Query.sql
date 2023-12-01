-- Performance Optimization: make queries run faster, save costs

-- Snowflake automatically manage following things:
-- - Add indexes, primary keys
-- - Create table partitions
-- - Managed micro-partitions
-- - Analyze the query execution table plan
-- - Remove unnecessary full table scans

-- Things to manage by yourself
-- - Assigning appropriate data types
-- - Sizing virtual warehouses
-- - Cluster keys

-- Our job
-- - Dedicated virtual warehouses - separated according to different workloads
-- - Scaling up - for known patterns of high workload
-- - Scaling out - dynamically for unknown patterns of workload
-- - Maximize chche usage - automatically caching can be maximized
-- - Cluster keys - for large tables

-----------------------------------------------------------------------------------------------------------------------

---- Virtual Warehouses ----

-- Identify and classify groups of workload/users
-- - BI team, Data Science team, Marketing team, Reporting team
-- Create dedicated warehouse for different kinds of workload/users groups
-- Not too many warehouses - avoid underutilization
-- Refine classification - work patterns can change

-- Create virtual warehouse for DS
CREATE WAREHOUSE DS_WH 
WITH WAREHOUSE_SIZE = 'SMALL'
WAREHOUSE_TYPE = 'STANDARD' 
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

-- Create virtual warehouse for DBA
CREATE WAREHOUSE DBA_WH 
WITH WAREHOUSE_SIZE = 'XSMALL'
WAREHOUSE_TYPE = 'STANDARD'
AUTO_SUSPEND = 300 
AUTO_RESUME = TRUE 
MIN_CLUSTER_COUNT = 1 
MAX_CLUSTER_COUNT = 1 
SCALING_POLICY = 'STANDARD';

-- Create role for Data Scientists & DBAs
CREATE ROLE DATA_SCIENTIST;
GRANT USAGE ON WAREHOUSE DS_WH TO ROLE DATA_SCIENTIST;

CREATE ROLE DBA;
GRANT USAGE ON WAREHOUSE DBA_WH TO ROLE DBA;

-- Setting up users with roles

-- Data Scientists
CREATE USER DS1
PASSWORD = 'DS1'
LOGIN_NAME = 'DS1'
DEFAULT_ROLE='DATA_SCIENTIST'
DEFAULT_WAREHOUSE = 'DS_WH'
MUST_CHANGE_PASSWORD = FALSE;

CREATE USER DS2
PASSWORD = 'DS2'
LOGIN_NAME = 'DS2'
DEFAULT_ROLE='DATA_SCIENTIST'
DEFAULT_WAREHOUSE = 'DS_WH'
MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DATA_SCIENTIST TO USER DS1;
GRANT ROLE DATA_SCIENTIST TO USER DS2;

-- DBAs
CREATE USER DBA1
PASSWORD = 'DBA1'
LOGIN_NAME = 'DBA1'
DEFAULT_ROLE='DBA'
DEFAULT_WAREHOUSE = 'DBA_WH'
MUST_CHANGE_PASSWORD = FALSE;

CREATE USER DBA2
PASSWORD = 'DBA2'
LOGIN_NAME = 'DBA2'
DEFAULT_ROLE='DBA'
DEFAULT_WAREHOUSE = 'DBA_WH'
MUST_CHANGE_PASSWORD = FALSE;

GRANT ROLE DBA TO USER DBA1;
GRANT ROLE DBA TO USER DBA2;

-----------------------------------------------------------------------------------------------------------------------

---- Scaling Up/Down ----

-- Changing size of the VWH depending on different workloads in different periods.
-- Use-cases:
-- - ETL at certain times
-- - Special business event with more workload
-- - NOTE: Common scenario is increased query complexity not more users; then Scaling out would be better

-- Query for Scaling Up/Down
ALTER WAREHOUSE <warehouse_name>
    SET WAREHOUSE_SIZE = 'SMALL';

-----------------------------------------------------------------------------------------------------------------------

---- Scaling Out/In ----

-- Using additional warehouses/multi-cluster warehouses
-- Common scenario is more concurrent users/queries
-- Automation the process if you have fluctuating number of users
-- Considerations:
-- - If you use at least Enterprise Edition all VWHs should be Multi-cluster
-- - Minimum: Default should be 1
-- - Maximum: Can be very high

-----------------------------------------------------------------------------------------------------------------------

---- Caching ----

-- What Snowflake do?
-- - Automatical process to speed up the queries
-- - If query is executed twice, results are cached and can be re-used
-- - Results are cached for 24 hours or until underlaying data has changed

-- What can we do?
-- - Ensure that similar queries go on the same warehouse
-- - Example: Team of DS run similar queries, so they should all use the same warehouse.

-----------------------------------------------------------------------------------------------------------------------

---- Clustering in Snowflake ----

-- What is a cluster key?
-- - Subset of rows to locate the data in micro-partitions
-- - For large tables this improves the scan efficiency in our queries
-- - Snowflake automatically maintains these cluster keys
-- - In general Snowflake produces well-clustered tables
-- - Cluster keys are not always ideal and can chnage over time
-- - Manually customize these cluster keys

-- When to cluster?
-- - Clustering is not for all tables
-- - Mainly very large tables of multiple TBs can benefit from cluster keys

-- How to cluster?
-- - Columns that are used most frequently in WHERE-clauses (often date columns for event tables)
-- - If you typically use filters on two columns then the table can also benefit from two cluster keys
-- - Column that is frequently used in Joins
-- - Large enough number of distinct values to enable effective grouping
-- - Small enough number of distinct values to enable effective grouping

-- Clustering in Snowflake

-- Defining a Clustering Key for a Table
CREATE TABLE <table_name> ... CLUSTER BY ( <column1> [ , <column2> ... ] );

-- Changing the Clustering Key for a Table
ALTER TABLE <table_name> CLUSTER BY ( <expr1> [ , <expr2> ... ] );

-- Dropping the Clustering Keys for a Table
ALTER TABLE <table_name> DROP CLUSTERING KEY;

-----------------------------------------------------------------------------------------------------------------------