-- Create the Project Infrastructure
use role sysadmin;
create or replace database AGS_GAME_AUDIENCE;
drop schema public;
create schema raw;

-- Create a Table
create or replace table AGS_GAME_AUDIENCE.RAW.GAME_LOGS (
    raw_log variant
    );

select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Create a Stage
create or replace stage uni_kishore
    url = 's3://uni-kishore';

-- Test the Stage
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;

-- Create a File Format
create or replace file format FF_JSON_LOGS
    TYPE = 'JSON'
    strip_outer_array = true;

-- Explore file before loading it
select $1 from @uni_kishore/kickoff
(file_format => FF_JSON_LOGS);

-- Load the File Into The Table
copy  into ags_game_audience.raw.game_logs from @uni_kishore/kickoff
file_format = (format_name = FF_JSON_LOGS);

-- Check loaded files in the table
select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Select Statement that Separates Every Attribute into It's Own Column
select 
    raw_log:agent::text as agent,
    raw_log:user_event::text as user_agent,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    *
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Create view as LOGS
create or replace view logs as (
select 
    raw_log:agent::text as agent,
    raw_log:user_event::text as user_agent,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    *
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
);

select * from AGS_GAME_AUDIENCE.RAW.LOGS;

----------------------------------------------------------------------------------------------------------------------------

-- Change the Time Zone for Your Current Worksheet

-- What time zone is your account(and/or session) currently set to? Is it -0700?
select current_timestamp();   -- Yes

-- Worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();   -- Right now it is +0000

-- How did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();   -- Right now it is +0300

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();   -- Right now it is +1200

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();   -- Right now it is +0800

-- Show the account parameter called timezone
show parameters like 'timezone';

----------------------------------------------------------------------------------------------------------------------------

-- CHALLENGE: Update Your Process to Accommodate the New File

-- New file Agnie downloaded from the game platform by listing files in the stage
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;

-- Access data in the stage folder : updated_feed
select $1 from @uni_kishore/updated_feed
(file_format => FF_JSON_LOGS);

-- Load the File Into The Table
copy  into ags_game_audience.raw.game_logs from @uni_kishore/updated_feed
file_format = (format_name = FF_JSON_LOGS);

select * from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- CHALLENGE: Filter Out the Old Rows
select 
    raw_log:agent::text as agent,
    raw_log:ip_address:: text as ip_address
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

select 
    raw_log:agent::text as agent,
    raw_log:ip_address:: text as ip_address
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where agent is null;

-- CHALLENGE: Update Your LOG View
create or replace view logs as (
select
    raw_log:user_event::text as user_agent,
    raw_log:user_login::text as user_login,
    raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601,
    raw_log:ip_address:: text as ip_address,
    *
from AGS_GAME_AUDIENCE.RAW.GAME_LOGS
where ip_address is not null
);

select * from AGS_GAME_AUDIENCE.RAW.LOGS;

-- Find Prajina's Log Events in Your Table
select * from AGS_GAME_AUDIENCE.RAW.LOGS
WHERE USER_LOGIN ilike '%Prajina%';

-- Use Snowflake's PARSE_IP Function
select parse_ip('100.41.16.160','inet');

-- Pull Out PARSE_IP Results Fields
select parse_ip('107.217.231.17','inet'):host;
select parse_ip('107.217.231.17','inet'):family;
select parse_ip('100.41.16.160','inet'):ip_fields;

----------------------------------------------------------------------------------------------------------------------------

-- Enhancement Infrastructure

-- Create a new Schema - ENHANCED
create schema ENHANCED;

-- Look Up Kishore & Prajina's Time Zone
-- Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

-- Look Up Everyone's Time Zone
-- Join the log and location tables to add time zone to each row using the PARSE_IP function
select logs.*, loc.city, loc.region, loc.country, loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;

----------------------------------------------------------------------------------------------------------------------------

-- Functions As Part of the Share

-- TO_JOIN_KEY function : reduces the IP Down to an integer that is helpful for joining with a range of rows that match our IP Address
-- TO_INT function converts IP Addresses to integers so we don't have to try to compare them as strings

-- Use the IPInfo Functions for a More Efficient Lookup

--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
--Use two functions supplied by IPShare to help with an efficient IP Lookup Process!
SELECT 
    logs.ip_address,
    logs.user_login,
    logs.user_agent,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

-- Create a Local Time Column!
SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_agent,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

-- Add A Column Called DOW_NAME!
SELECT
    logs.ip_address,
    logs.user_login,
    logs.user_agent,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz,
    dayname(to_date(game_event_ltz)) as DOW_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

----------------------------------------------------------------------------------------------------------------------------

-- Assigning a Time of Day

-- Your role should be SYSADMIN
-- Your database menu should be set to AGS_GAME_AUDIENCE
-- The schema should be set to RAW

-- A Look Up table to convert from hour number to "time of day name"
create table ags_game_audience.raw.time_of_day_lu (
    hour number,
    tod_name varchar(25)
    );

-- Insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
    (6,'Early morning'),
    (7,'Early morning'),
    (8,'Early morning'),
    (9,'Mid-morning'),
    (10,'Mid-morning'),
    (11,'Late morning'),
    (12,'Late morning'),
    (13,'Early afternoon'),
    (14,'Early afternoon'),
    (15,'Mid-afternoon'),
    (16,'Mid-afternoon'),
    (17,'Late afternoon'),
    (18,'Late afternoon'),
    (19,'Early evening'),
    (20,'Early evening'),
    (21,'Late evening'),
    (22,'Late evening'),
    (23,'Late evening'),
    (0,'Late at night'),
    (1,'Late at night'),
    (2,'Late at night'),
    (3,'Toward morning'),
    (4,'Toward morning'),
    (5,'Toward morning');

-- Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') from time_of_day_lu
group by tod_name;

-- A Join with a Function 
SELECT 
    logs.ip_address,
    logs.user_login,
    logs.user_agent,
    logs.datetime_iso8601,
    city,
    region,
    country,
    timezone,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz,
    dayname(to_date(game_event_ltz)) as DOW_name,
    tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu
on time_of_day_lu.hour = hour(game_event_ltz);

-- Rename the Columns
SELECT 
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_agent as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz,
    dayname(to_date(game_event_ltz)) as DOW_name,
    tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu
on time_of_day_lu.hour = hour(game_event_ltz);

-- Convert a Select to a Table
-- Wrap any Select in a CTAS statement
create or replace table ags_game_audience.enhanced.logs_enhanced as (
SELECT 
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_agent as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz,
    dayname(to_date(game_event_ltz)) as DOW_name,
    tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu
on time_of_day_lu.hour = hour(game_event_ltz)
);

-- Check Your Table
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

----------------------------------------------------------------------------------------------------------------------------

-- Create a Simple Task
create task load_logs_enhanced
    warehouse = 'compute_wh'
    schedule = '5 minute'
    as select 'hello';

-- SYSADMIN Privileges for Executing Tasks
-- You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
-- This is true even if SYSADMIN owns the task !
grant execute task on account to role SYSADMIN;

--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

----------------------------------------------------------------------------------------------------------------------------

-- Making the Task Better

-- Use the CTAS Logic in the Task
create or replace task load_logs_enhanced
    warehouse = 'compute_wh'
    schedule = '5 minute'
    as select 
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz,
    dayname(to_date(game_event_ltz)) as DOW_name,
    tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc 
on IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
between start_ip_int and end_ip_int
join ags_game_audience.raw.time_of_day_lu
on time_of_day_lu.hour = hour(game_event_ltz);

-- Executing the Task to Load More Rows
-- Make a note of how many rows you have in the table
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

----------------------------------------------------------------------------------------------------------------------------

-- Convert Your Task so It Inserts Rows
create or replace task load_logs_enhanced
    warehouse = 'compute_wh'
    schedule = '5 minute'
    as 
    insert into AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
    select 
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    convert_timezone('UTC', timezone, datetime_iso8601) as game_event_ltz,
    dayname(to_date(game_event_ltz)) as DOW_name,
    tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc 
on IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
between start_ip_int and end_ip_int
join ags_game_audience.raw.time_of_day_lu
on time_of_day_lu.hour = hour(game_event_ltz);

-- Execute task manually
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Check to see how many rows were added
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

----------------------------------------------------------------------------------------------------------------------------

-- Trunc & Reload Like It's Y2K!

-- First we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

-- Then we put them all back in
insert into ags_game_audience.enhanced.LOGS_ENHANCED (
select 
    logs.ip_address,
    logs.user_login as GAMER_NAME,
    logs.user_event as GAME_EVENT_NAME,
    logs.datetime_iso8601 as GAME_EVENT_UTC,
    city,
    region,
    country,
    timezone as GAMER_LTZ_NAME,
    CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz,
    DAYNAME(game_event_ltz) as DOW_NAME,
    TOD_NAME
from ags_game_audience.raw.LOGS logs
join ipinfo_geoloc.demo.location loc 
on ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
and ipinfo_geoloc.public.TO_INT(logs.ip_address) 
between start_ip_int and end_ip_int
join ags_game_audience.raw.TIME_OF_DAY_LU tod
on hour(game_event_ltz) = tod.hour);

-- Create a Backup Copy of the Table

-- Clone the table to save this version as a backup
-- Since it holds the records from the UPDATED FEED file, we'll name it _UF
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
    clone ags_game_audience.enhanced.LOGS_ENHANCED;

-- Sophisticated 2010's - The Merge!
merge into ENHANCED.LOGS_ENHANCED e
using RAW.LOGS r
on r.user_login = e.GAMER_NAME
and r.datetime_iso8601 = e.game_event_utc
and r.user_event = e.GAME_EVENT_NAME
when matched then
update set IP_ADDRESS = 'Hey I updated matching rows!';

----------------------------------------------------------------------------------------------------------------------------

-- Truncate Again for a Fresh Start

-- Let's truncate so we can start the load over again
-- Remember we have that cloned back up so it's fine
truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

-- Build your Insert Merge with Task
create or replace task load_logs_enhanced
    warehouse = 'compute_wh'
    schedule = '5 minute'
    as 
merge into ENHANCED.LOGS_ENHANCED e
using (
        select 
        logs.ip_address,
        logs.user_login as GAMER_NAME,
        logs.user_event as GAME_EVENT_NAME,
        logs.datetime_iso8601 as GAME_EVENT_UTC,
        city,
        region,
        country,
        timezone as GAMER_LTZ_NAME,
        CONVERT_TIMEZONE( 'UTC',timezone,logs.datetime_iso8601) as game_event_ltz,
        DAYNAME(game_event_ltz) as DOW_NAME,
        TOD_NAME
        from ags_game_audience.raw.LOGS logs
        join ipinfo_geoloc.demo.location loc 
        on ipinfo_geoloc.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        and ipinfo_geoloc.public.TO_INT(logs.ip_address) 
        between start_ip_int and end_ip_int
        join ags_game_audience.raw.TIME_OF_DAY_LU tod
        on HOUR(game_event_ltz) = tod.hour) r
on r.GAMER_NAME = e.GAMER_NAME
and r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
and r.GAME_EVENT_NAME = e.GAME_EVENT_NAME
when not matched then
insert (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, 
        GAME_EVENT_UTC, CITY, REGION, COUNTRY,
        GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME,
        TOD_NAME)
values (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, 
        GAME_EVENT_UTC, CITY, REGION, COUNTRY,
        GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME,
        TOD_NAME);

-- Execute task  
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- View table after task runs
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;