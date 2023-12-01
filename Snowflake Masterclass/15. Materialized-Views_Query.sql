-- If we have a VIEW that is queried frequently and that a long time to processed.
-- - Bad user experience
-- - More compute consumption
-- We can create a materialized view to solve that problem.
-- Use any SELECT statement to create this Materialized View
-- Results will be stored in a separate table and automatically updated based on base table.
-- Only available for Enterprise edition or higher.
-- Joins (including Self-joins) are not supported.
-- Limited amount of aggregation functions.(Refer documentation)
-- Can't use UDF, HAVING, ORDER BY and LIMIT clauses
-- Materialized View is totally managed by the Snowflake. 

-----------------------------------------------------------------------------------------------------------------------

-- Remove caching for fair test
ALTER SESSION SET USE_CACHED_RESULT = FALSE;  -- disable global caching

ALTER WAREHOUSE <warehouse_name> SUSPEND;
ALTER WAREHOUSE <warehouse_name> RESUME;

-----------------------------------------------------------------------------------------------------------------------

-- Prepare table with huge data
-- Create normal view

-- Create materialized view
CREATE OR REPLACE MATERIALIZED VIEW <m_view_name>
AS
    -- SELECT Query
    ;

-- Show Materialized Views
SHOW MATERIALIZED VIEWS;

-- SELECT on Materialized Views
SELECT * FROM <m_view_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Show Materialized Views refresh history and credits used for that
SELECT * FROM TABLE(INFORMATION_SCHEMA.MATERIALIZED_VIEW_REFRESH_HISTORY());

-----------------------------------------------------------------------------------------------------------------------

-- When to use Materialized View?
-- - View would take a long time to be processed and is used frequently
-- - Underlaying data is change not frequently and on a rather irregular basis
-- - Don't use materialized view if data changes are very frequent
-- - Keep maintenance cost in mind
-- - Consider leveraging tasks & streams instead

-- If the data is updated on a very regular basis OR Streaming objects ... 
-- - Using Tasks and Streams could be better alternative

-----------------------------------------------------------------------------------------------------------------------