-- Streams: Object that records (DML) changes (i.e. CDC) made to table.
-- CDC: Above process is called Change Data Capture.
-- Changes are DELETE, INSERT and UPDATE.
-- If these chnages occured in the table, that is recorded in stream object of that table.
-- Moreover, there are 3 additional columns are added in the stream object.
                            METADATA$ACTION
                            METADATA$ISUPDATE
                            METADATA$ROW_ID
-- Stream objects are retrieve from the original table; only little more additional storage cost for 3 columns.
-- Once we insert stream object data (i.e. additional data) to target table; then data deleted from the stream object.

-----------------------------------------------------------------------------------------------------------------------
-- There are two types of Streams i.e. Standard (default; all data changes) and Append-Only (only INSERT CDC)

-- Append-Only Syntax
CREATE OR REPLACE <stream_table> ON TABLE <table_name>
    APPEND_ONLY = TRUE;

-- View all stream objects
SHOW STREAMS;
-- Mode column has specification regarding Stream type

-----------------------------------------------------------------------------------------------------------------------

-- Create Stream object
CREATE OR REPLACE STREAM <stream_table> ON TABLE <table_name>;
-- Once you create stream object after it start capture data changes.

-- View all stream objects
SHOW STREAMS;

-- Describe Stream
DESC STREAM <stream_table>

-- Select query on stream object
SELECT * FROM <stream_table>;

-----------------------------------------------------------------------------------------------------------------------

-- Insert operation
METADATA$ACTION = INSERT
METADATA$ISUPDATE = FALSE
METADATA$ROW_ID = <ROW_ID>

-- Suppose there is insert operation on perticular table and that CDC is recorded on stream object for that table.
-- Now insert that stream object data to target table using stream object as follow:
INSERT INTO <target_table>
    SELECT column1, column2, column3, column4 FROM <stream_table>
    JOIN
    <source_table> ON <source_table>.ID = <stream_table>.ID;
-- Now stream object has been consume to target table.
-- Therefore, if we run SELECT statement on stream object, results will be empty.
SELECT * FROM <stream_table>;

-----------------------------------------------------------------------------------------------------------------------

-- Update operation
METADATA$ACTION = DELETE AND INSERT
METADATA$ISUPDATE = TRUE (same on both DELETE and INSERT)
METADATA$ROW_ID = <ROW_ID> (same on both DELETE and INSERT)

-- Suppose there is update operation on perticular table and that CDC is recorded on stream object for that table.
-- Now Merge that stream object data to target table using stream object as follow:
MERGE INTO <target_table>
USING <stream_table>
    ON <target_table>.ID = <stream_table>.ID
WHEN MATCHED
    AND <stream_table>.METADATA$ACTION = 'INSERT'
    AND <stream_table>.METADATA$ISUPDATE = 'TRUE'
    THEN UPDATE
    SET <target_table>.column1 = <stream_table>.column1
        <target_table>.column2 = <stream_table>.column2
        <target_table>.column3 = <stream_table>.column3
-- Now stream object has been consume to target table.
-- Therefore, if we run SELECT statement on stream object, results will be empty.
SELECT * FROM <stream_table>;

-----------------------------------------------------------------------------------------------------------------------

-- Delete operation
METADATA$ACTION = DELETE
METADATA$ISUPDATE = FALSE
METADATA$ROW_ID = <ROW_ID>

-- Suppose there is delete operation on perticular table and that CDC is recorded on stream object for that table.
-- Now Merge that stream object data to target table using stream object as follow:
MERGE INTO <target_table>
USING <stream_table>
    ON <target_table>.ID = <stream_table>.ID
WHEN MATCHED
    AND <stream_table>.METADATA$ACTION = 'DELETE'
    AND <stream_table>.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE
-- Now stream object has been consume to target table.
-- Therefore, if we run SELECT statement on stream object, results will be empty.
SELECT * FROM <stream_table>;

-----------------------------------------------------------------------------------------------------------------------

-- INSERT, DELETE and UPDATE operation

-- Now Merge that stream object data to target table using stream object as follow:
MERGE INTO <target_table>       -- Target table to merge changes from source table
USING (SELECT <stream_table>.*, <source_table>.column1, <source_table>.column2
       FROM <stream_table>
       JOIN <source_table>
       ON <stream_table>.ID = <source_table>.ID
       ) S
ON <target_table>.ID = S.ID
WHEN MATCHED                                -- DELETE condition
    AND S.METADATA$ACTION = 'DELETE'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE
WHEN MATCHED                                -- UPDATE condition
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE = 'TRUE'
    THEN UPDATE
    SET <target_table>.column1 = S.column1
        <target_table>.column2 = S.column2
        <target_table>.column3 = S.column3
WHEN NOT MATCHED                            -- INSERT condition
    AND S.METADATA$ACTION = 'INSERT'
    THEN INSERT (column1, column2, column3)
    VALUES
        (S.column1, S.column2, S.column3);
-- Now stream object has been consume to target table.
-- Therefore, if we run SELECT statement on stream object, results will be empty.
SELECT * FROM <stream_table>;

-----------------------------------------------------------------------------------------------------------------------

-- Combine Streams & Tasks - For automate the process --

-- Automate the updates using Tasks --

-- Create task for all data changes
CREATE OR REPLACE TASK <task_name>
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('<stream_table>')  -- return TRUE or FALSE
    AS
MERGE INTO <target_table>                   -- Target table to merge changes from Source table
USING (SELECT <stream_table>.*, <source_table>.column1, <source_table>.column2
       FROM <stream_table>
       JOIN <source_table>
       ON <stream_table>.ID = <source_table>.ID
       ) S
ON <target_table>.ID = S.ID
WHEN MATCHED                                -- DELETE condition
    AND S.METADATA$ACTION = 'DELETE'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE
WHEN MATCHED                                -- UPDATE condition
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE = 'TRUE'
    THEN UPDATE
    SET <target_table>.column1 = S.column1
        <target_table>.column2 = S.column2
        <target_table>.column3 = S.column3
WHEN NOT MATCHED
    AND S.METADATA$ACTION = 'INSERT'
    THEN INSERT (column1, column2, column3)
    VALUES
        (S.column1, S.column2, S.column3);

-- Resume the task
ALTER TASK <task_name> RESUME;

-- Show all tasks
SHOW TASKS;

-- Now, implement some data changes on source table
-- Some INSERT, DELETE & UPADTE operations

-- Now stream object has been consume to target table automatically due to task.
-- Therefore, if we run SELECT statement on stream object, results will be empty.
SELECT * FROM <stream_table>;

-- Verify the task history
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
ORDER BY NAME ASC, SCHEDULED_TIME DESC;
-----------------------------------------------------------------------------------------------------------------------

-- CHANGE clause - alternative method to track changes in the table
-- Drawback - only return latest changes; after consuming the changes, data don't get deleted from changes.

-- Create table
CREATE OR REPLACE TABLE <table_name> (
    column1,
    column2,
    column3
);

-- Alter table to track changes
ALTER TABLE <table_name>
SET CHANGE_TRACKING = TRUE;

-- Track changes
SELECT * FROM <table_name>
CHANGES(INFORMATION  => DEFAULT | APPEND_ONLY)
AT (TIMESTAMP => 'your-timestamp'::timestamp_tz);

-- To consume changes
CREATE OR REPLACE TABLE <table_name> 
AS
    SELECT * FROM <table_name>
    CHANGES(INFORMATION  => DEFAULT | APPEND_ONLY)
    AT (TIMESTAMP => 'your-timestamp'::timestamp_tz);

-----------------------------------------------------------------------------------------------------------------------