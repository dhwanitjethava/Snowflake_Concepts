-- 1. Virtual Warehouse
--    - Enable Auto-suspend
        AUTO_SUSPEND = 0 (Warehouse will never suspend)
        AUTO_SUSPEND = 1 (for fast suspension)
--    - Enable Auto-resume
        AUTO_RESUME = TRUE
--    - Set appropriate timeouts
--      - For ETL/Data Loading - Immediately
--      - BI/SELECT queries - 10 min
--      - DevOps/Data Science - 5 min

-----------------------------------------------------------------------------------------------------------------------

-- 2. Table Design
--    - Choose appropriate table type
--      - Staging tables - Transient
--      - Development tables - Transient
--      - Productive tables - Permanent
--    - Choose appropriate datatype
--    - Set cluster keys only if necesarry
--      - if we have very large table
--      - if most of the query time used for table scan
--      - if we want to query against dimensions (for transactional data usually sorted by date, but if we want to
--        query in order with region, that is against dimension)

-----------------------------------------------------------------------------------------------------------------------

-- 3. Monitoring Storage & Compute usage 
--    - Table Storage matrics
        SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_MATRICS;
--    - How much is queried in databases
        SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY;
        SELECT 
            DATABASE_NAME,
            COUNT(*) AS NUMBER_OF_QUERIES,
            SUM(CREDITS_USED_CLOUD_SERVICES),
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        GROUP BY DATABASE_NAME;
--    - Usage of credits by warehouses
        SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY;
--    - Usage of credits by warehouses (Grouped by day)
        SELECT 
            DATE(START_TIME),
            SUM(CREDITS_USED)
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        GROUP BY DATE(START_TIME);
--    - Usage of credits by warehouses (Grouped by warehouse)
        SELECT
            WAREHOUSE_NAME,
            SUM(CREDITS_USED)
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        GROUP BY WAREHOUSE_NAME;
--    - Usage of credits by warehouses (Grouped by warehouse & day)
        SELECT
            DATE(START_TIME),
            WAREHOUSE_NAME,
            SUM(CREDITS_USED)
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        GROUP BY WAREHOUSE_NAME,DATE(START_TIME);

-----------------------------------------------------------------------------------------------------------------------

-- 4. Retention Period
--    - For staging database - 0 day (transient table)
--    - For production - 4 to 7 days (minimum 1 day)
--    - Large high-churn(storage) tables - 0 day (transient table)

-----------------------------------------------------------------------------------------------------------------------