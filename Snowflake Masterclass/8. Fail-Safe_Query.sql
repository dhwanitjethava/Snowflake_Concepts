-- Fail Safe
-- Protection of historical data in case of disaster
-- For Permanent Table - 7 days period (Non configurable)
-- For Transient Table - 0 days period (Non configurable)
-- Fail Safe period starts immediately after Time Travel period ends
-- No user interaction & recoverable, only by Snowflake (recovery beyond Time Travel)
-- Contributes to storage cost

-----------------------------------------------------------------------------------------------------------------------

-- Fail Safe Storage

-- Storage usage on account level
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;


-- Storage usage on account level (formatted)
SELECT USAGE_DATE, 
       STORAGE_BYTES / (1024*1024*1024) AS STORAGE_GB,  
	   STAGE_BYTES / (1024*1024*1024) AS STAGE_GB,
	   FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.STORAGE_USAGE ORDER BY USAGE_DATE DESC;

-- Storage usage on table level
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;

-- Storage usage on table level (formatted)
SELECT ID, 
	   TABLE_NAME, 
	   TABLE_SCHEMA,
	   ACTIVE_BYTES / (1024*1024*1024) AS STORAGE_USED_GB,
	   TIME_TRAVEL_BYTES / (1024*1024*1024) AS TIME_TRAVEL_STORAGE_USED_GB,
	   FAILSAFE_BYTES / (1024*1024*1024) AS FAILSAFE_STORAGE_USED_GB
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
ORDER BY FAILSAFE_STORAGE_USED_GB DESC;

-----------------------------------------------------------------------------------------------------------------------