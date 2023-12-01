-- Create copies of a database, a schema or a table without need for additional storage or long waiting periods.
-- Clone command is meta-data operation.
-- Cloned object is independent from original table.
-- Cloning works with time-travel also.
-- Cloning is not working with temporary table but, cloning of temporary table with temporary clone works.
-- If we delete source table; clone table is still exist.
-- Advantage: Easy to copy all meta-data & improved storage management.
-- Advantage: Creating backups for development purposes.

-----------------------------------------------------------------------------------------------------------------------

-- Cloning Table
CREATE OR REPLACE TABLE <clone_table_name>
CLONE <original_table_name>;

-- Cloning Schema
CREATE OR REPLACE SCHEMA <clone_schema_name>
CLONE <original_schema_name>;

-- Cloning Database
CREATE OR REPLACE DATABASE <clone_database_name>
CLONE <original_database_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Cloning with Time-travel

CREATE OR REPLACE TABLE <clone_table_name>
CLONE <original_table_name> AT(OFFSET => -60*5);

-----------------------------------------------------------------------------------------------------------------------

-- Swapping Tables
-- Use-case: Development table into production table

ALTER TABLE <table_name>
    SWAP WITH <target_table_name>;

ALTER SCHEMA <schema_name>
    SWAP WITH <target_schema_name>;

-----------------------------------------------------------------------------------------------------------------------