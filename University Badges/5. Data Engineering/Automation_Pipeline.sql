-- Create A New Stage and a New Target Table!

-- 1) Stage: UNI_KISHORE_PIPELINE
-- 2) URL: s3://uni-kishore-pipeline
-- 3) Put this stage in the RAW schema

create or replace stage UNI_KISHORE_PIPELINE
    url = 's3://uni-kishore-pipeline';

-- Look at the files in the new stage using LIST
list @UNI_KISHORE_PIPELINE;

-- Create a table called PIPELINE_LOGS (same structure as GAME_LOGS table)
create or replace TABLE AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS (
	RAW_LOG VARIANT
);

-- Load the File Into The Table
copy  into ags_game_audience.raw.PIPELINE_LOGS from @UNI_KISHORE_PIPELINE
file_format = (format_name = FF_JSON_LOGS);

-- How many rows in table PIPELINE_LOGS
select * from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

-- Create a New PL_LOGS View (same structure as LOGS view)
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS as
select
    raw_log:user_event::text as user_event,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log:ip_address:: text as ip_address,
    *
from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

-- View PL_LOGS views
select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

-------------------------------------------------------------------------------------------------------------------------------------------

-- Create a TASK to run the COPY INTO

-- Create a Task that runs every 5 minutes
-- Name task GET_NEW_FILES (put it in the RAW schema)
-- Copy and paste your COPY INTO into the body of your GET_NEW_FILES task 
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
        warehouse=COMPUTE_WH
	schedule='5 minute'
	as copy  into ags_game_audience.raw.PIPELINE_LOGS from @UNI_KISHORE_PIPELINE
        file_format = (format_name = FF_JSON_LOGS);

-- Execute task  
execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

-- View table after task runs
select * from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

-- Truncate The Target Table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Turn on Your Tasks
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

-------------------------------------------------------------------------------------------------------------------------------------------

-- Keep this code handy for shutting down the tasks each day
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

-------------------------------------------------------------------------------------------------------------------------------------------

-- Keeping Tallies in Mind

-- 1. Check the number of files in the stage, and multiply by 10. This is how many rows you should be expecting.
-- 2. The GET_NEW_FILES task grabs files from the UNI_KISHORE_PIPELINE stage and loads them into PIPELINE_LOGS.
--    How many rows are in PIPELINE_LOGS? 
-- 3. The PL_LOGS view normalizes PIPELINE_LOGS without moving the data. Even though there are some filters in
--    the view, we don't expect to lose any rows. How many rows are in PL_LOGS?
-- 4. The LOAD_LOGS_ENHANCED task uses the PL_LOGS view and 3 tables to enhance the data. 
--    We don't expect to lose any rows. How many rows are in LOGS_ENHANCED?

-- Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

-- Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

-- Step 3 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

-- Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-------------------------------------------------------------------------------------------------------------------------------------------

-- Keep this code handy for shutting down the tasks each day
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

-------------------------------------------------------------------------------------------------------------------------------------------

-- A New Select with Metadata and Pre-Load JSON Parsing

select 
    METADATA$FILENAME as log_file_name,                     --new metadata column
    METADATA$FILE_ROW_NUMBER as log_file_row_id,            --new metadata column
    current_timestamp(0) as load_ltz,                       --new local time of load
    get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
    get($1,'user_event')::text as USER_EVENT,
    get($1,'user_login')::text as USER_LOGIN,
    get($1,'ip_address')::text as IP_ADDRESS    
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');

-- Create a New Target Table to Match the Select (Using CTAS)

-- Create a new logs table in the RAW schema and call it ED_PIPELINE_LOGS
-- You could write this table definition any way you want 
create or replace table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS as
select 
    METADATA$FILENAME as log_file_name,                     --new metadata column
    METADATA$FILE_ROW_NUMBER as log_file_row_id,            --new metadata column
    current_timestamp(0) as load_ltz,                       --new local time of load
    get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
    get($1,'user_event')::text as USER_EVENT,
    get($1,'user_login')::text as USER_LOGIN,
    get($1,'ip_address')::text as IP_ADDRESS    
from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');

-- Create the New COPY INTO

-- Truncate the table rows that were input during the CTAS
truncate table ED_PIPELINE_LOGS;

-- Reload the table using your COPY INTO
copy into ED_PIPELINE_LOGS from (
    select 
        METADATA$FILENAME as log_file_name,
        METADATA$FILE_ROW_NUMBER as log_file_row_id,,
        current_timestamp(0) as load_ltz, 
        get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
        get($1,'user_event')::text as USER_EVENT,
        get($1,'user_login')::text as USER_LOGIN,
        get($1,'ip_address')::text as IP_ADDRESS
    from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE)
file_format = (format_name = ff_json_logs);

-------------------------------------------------------------------------------------------------------------------------------------------

-- Create Your Snowpipe!

create or replace pipe GET_NEW_FILES
        auto_ingest=true
        aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
as 
copy into ED_PIPELINE_LOGS from (
        select 
              METADATA$FILENAME as log_file_name, 
              METADATA$FILE_ROW_NUMBER as log_file_row_id, 
              current_timestamp(0) as load_ltz,
              get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601,
              get($1,'user_event')::text as USER_EVENT,
              get($1,'user_login')::text as USER_LOGIN,
              get($1,'ip_address')::text as IP_ADDRESS    
        from @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
        )
file_format = (format_name = ff_json_logs);

-- Alter task 
create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
	warehouse=COMPUTE_WH
	schedule='5 minute'
	as copy  into ags_game_audience.raw.ED_PIPELINE_LOGS from @UNI_KISHORE_PIPELINE
        file_format = (format_name = FF_JSON_LOGS);

alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
ALTER PIPE GET_NEW_FILES SET PIPE_EXECUTION_PAUSED = FALSE;

-------------------------------------------------------------------------------------------------------------------------------------------

-- Create a Stream

-- Create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

-- Look at the stream you created
show streams;

-- Check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

-- View Our Stream Data

-- Query the stream
select * from ags_game_audience.raw.ed_cdc_stream; 

-- Check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

-- If your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('GET_NEW_FILES');

-- If you need to pause or unpause your pipe
-- Alter pipe GET_NEW_FILES set pipe_execution_paused = true;
-- Alter pipe GET_NEW_FILES set pipe_execution_paused = false;

-- Process the Rows from the Stream

-- Make a note of how many rows are in the stream
select * from ags_game_audience.raw.ed_cdc_stream; 

-- Process the stream by using the rows in a merge 
merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
        select cdc.ip_address,
               cdc.user_login as GAMER_NAME,
               cdc.user_event as GAME_EVENT_NAME,
               cdc.datetime_iso8601 as GAME_EVENT_UTC,
               city,
               region,
               country,
               timezone as GAMER_LTZ_NAME,
               CONVERT_TIMEZONE('UTC',timezone,cdc.datetime_iso8601) as game_event_ltz,
               DAYNAME(game_event_ltz) as DOW_NAME,
               TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        join ipinfo_geoloc.demo.location loc 
        on ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        and ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        between start_ip_int AND end_ip_int
        join AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        on hour(game_event_ltz) = tod.hour
      ) r
on r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
when not matched then 
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY, REGION,
        COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME)
        values
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY, REGION,
        COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME);
 
--Did all the rows from the stream disappear? 
select * from ags_game_audience.raw.ed_cdc_stream;  -- YES

-- Create a CDC-Fueled, Time-Driven Task

-- Turn off the other task (we won't need it anymore)
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

-- Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
	as 
merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
        select cdc.ip_address,
               cdc.user_login as GAMER_NAME,
               cdc.user_event as GAME_EVENT_NAME,
               cdc.datetime_iso8601 as GAME_EVENT_UTC,
               city,
               region,
               country,
               timezone as GAMER_LTZ_NAME,
               CONVERT_TIMEZONE('UTC',timezone,cdc.datetime_iso8601) as game_event_ltz,
               DAYNAME(game_event_ltz) as DOW_NAME,
               TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        join ipinfo_geoloc.demo.location loc 
        on ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        and ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        between start_ip_int AND end_ip_int
        join AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        on hour(game_event_ltz) = tod.hour
      ) r
on r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
when not matched then 
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY, REGION,
        COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME)
        values
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY, REGION,
        COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME);
        
-- Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

-- Add A Stream Dependency to the Task Schedule

create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
when 
    system$stream_has_data('ed_cdc_stream') as	 
merge into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
using (
        select cdc.ip_address,
               cdc.user_login as GAMER_NAME,
               cdc.user_event as GAME_EVENT_NAME,
               cdc.datetime_iso8601 as GAME_EVENT_UTC,
               city,
               region,
               country,
               timezone as GAMER_LTZ_NAME,
               CONVERT_TIMEZONE('UTC',timezone,cdc.datetime_iso8601) as game_event_ltz,
               DAYNAME(game_event_ltz) as DOW_NAME,
               TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        join ipinfo_geoloc.demo.location loc 
        on ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        and ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        between start_ip_int AND end_ip_int
        join AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        on hour(game_event_ltz) = tod.hour
      ) r
on r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
when not matched then 
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY, REGION,
        COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME)
        values
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME,
        GAME_EVENT_UTC, CITY, REGION,
        COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ,
        DOW_NAME, TOD_NAME);

-- Truncate the target table
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Check that the target table is empty
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Resume the task
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

-- Check that the target table is loading again
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Turn Things off
alter pipe GET_NEW_FILES set pipe_execution_paused = true;