-- How to Test Whether You Set Up Your Table in the Right Place with the Right Name

-- We can "ask" the Information Schema Table called "Tables" if our table exists by asking it to count 
-- the number of times a table with that name, in a certain schema, in a certain database (catalog) exists.
-- If it exists, we should get back the count of 1. 
select count(*) as OBJECTS_FOUND
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;

-- How to Test That You Loaded the Expected Number of Rows

-- We can "ask" the Information Schema Table called "Tables" if our table has the expected number
-- of rows with a command like this:
select row_count
from <database name>.INFORMATION_SCHEMA.TABLES 
where table_schema=<schema name> 
and table_name= <table name>;

-- If you want to share, but there are restrictions; try if you lucky
-- This will gives error, if you did not meet snowflake account edition requirements
use role accountadmin;
grant override share restrictions on account to role accountadmin;

SHOW RESOURCE MONITORS;