-- Task: Tasks can be used to schedule SQL statements.
-- Tasks can be standalone and trees of tasks.

-----------------------------------------------------------------------------------------------------------------------

-- Create Task
CREATE OR REPLACE TASK <task_name>
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'  -- '<num> MINUTE' || 'USING CRON * * * * * UTC'
AS 
    <single_sql_statement> || <call_to_stored_procedure>;

-----------------------------------------------------------------------------------------------------------------------

-- Using CRON expression

# ----------- minute (0-59)
# | --------- hour (0-23)
# | | ------- day of month (1-31, or L)
# | | | ----- month (1-12, JAN-DEC)
# | | | | --- day of week (0-6, SUN-SAT, or L)
# | | | | |
# | | | | |
  * * * * *

-- Every minute
SCHEDULE = 'USING CRON * * * * * UTC';

-- Every day at 6am UTC timezone
SCHEDULE = 'USING CRON 0 6 * * * UTC';

-- Every hour starting at 9 AM and ending at 5 PM on Sundays
SCHEDULE = 'USING CRON 0 9-17 * * SUN America/Los_Angeles';

-----------------------------------------------------------------------------------------------------------------------

-- Lists the tasks
SHOW TASKS;

-- Task starting and suspending
ALTER TASK <task_name> RESUME;
ALTER TASK <task_name> SUSPEND;

-----------------------------------------------------------------------------------------------------------------------
-- Upto 100 child tasks for one parent task.
-- One complete tree of tasks can include upto 1000 tasks.

-- First, suspend the parent task for creating child task
ALTER TASK <parent_task_name> SUSPEND;

-- Create child task
CREATE OR REPLACE TASK <child_task_name>
    WAREHOUSE = COMPUTE_WH
    AFTER <parent_task_name>
AS
    <single_sql_statement> || <call_to_stored_procedure>;

-----------------------------------------------------------------------------------------------------------------------

-- Calling a Stored procedure

-- Create a stored procedure - Example
CREATE OR REPLACE PROCEDURE CUSTOMERS_INSERT_PROCEDURE (CREATE_DATE VARCHAR)
    RETURNS STRING NOT NULL
    LANGUAGE JAVASCRIPT
    AS
        $$
        var sql_command = 'INSERT INTO CUSTOMERS(CREATE_DATE) VALUES(:1);'
        snowflake.execute(
            {
            sqlText: sql_command,
            binds: [CREATE_DATE]
            });
        return "Successfully executed.";
        $$;

CREATE OR REPLACE TASK CUSTOMER_TAKS_PROCEDURE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
AS
    CALL  CUSTOMERS_INSERT_PROCEDURE (CURRENT_TIMESTAMP);

-----------------------------------------------------------------------------------------------------------------------

-- Task history & Error handling

-- Use Database in which task is present
USE DATABASE <database_name>;

-- Use the table function 'TASK_HISTORY()

-- Retrieve the 100 most recent task executions (completed, still running, or scheduled in the future)
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
    ORDER BY SCHEDULED_TIME;

-- Retrieve the 10 most recent executions of a specified task (completed, still running, or scheduled in the future)
-- scheduled within the last hour
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY (
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP()),
    RESULT_LIMIT => 10,
    TASK_NAME => '<task_name>')
    );

-- Retrieve the execution history for tasks within a specified 30 minute block of time within the past 7 days:
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY (
    SCHEDULED_TIME_RANGE_START => TO_TIMESTAMP_LTZ('2018-11-9 12:00:00.000 -0700'),
    SCHEDULED_TIME_RANGE_END => TO_TIMESTAMP_LTZ('2018-11-9 12:30:00.000 -0700'))
    );

-----------------------------------------------------------------------------------------------------------------------

-- Tasks with condition

-- Create task with condition
CREATE OR REPLACE TASK <task_name>
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN 1 = 2  -- Condition
AS
    <single_sql_statement>;

-----------------------------------------------------------------------------------------------------------------------