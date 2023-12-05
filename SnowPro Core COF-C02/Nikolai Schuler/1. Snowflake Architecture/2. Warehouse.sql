-- Create a warehouse
CREATE OR REPLACE WAREHOUSE <warehouse_name>
WITH
WAREHOUSE_SIZE = XSMALL
MIN_CLUSTER_COUNT = 1
MAX_CLUSTER_COUNT = 2
AUTO_RESUME = TRUE | FALSE
AUTO_SUSPEND = 300  -- (in seconds)
COMMENT = '<string>';

-- To use warehouse
USE WAREHOUSE <warehouse_name>;

-- Drop warehouse
DROP WAREHOUSE <warehouse_name>;

-- Alter warehouse
ALTER WAREHOUSE IF EXISTS <warehouse_name1> RENAME TO <warehouse_name2>;
ALTER WAREHOUSE <warehouse_name> RESUME;
ALTER WAREHOUSE <warehouse_name> SET WAREHOUSE_SIZE = MEDIUM;
