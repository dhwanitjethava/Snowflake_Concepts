-- Set Up the ADU Decode Database and Schemas
use role SYSADMIN;

--Max created a database to store Vehicle Identification Numbers
create database max_vin;

drop schema max_vin.public;
create schema max_vin.decode;

-----------------------------------------------------------------------------------------------------

-- Max's Decode Tables

--We need a table that will allow WMIs to be decoded into Manufacturer Name, Country and Vehicle Type
create table MAX_VIN.DECODE.WMITOMANUF (
    WMI VARCHAR(6),
    MANUF_ID NUMBER(6),
    MANUF_NAME VARCHAR(50),
    COUNTRY VARCHAR(50),
    VEHICLETYPE VARCHAR(50)
    );
 
--We need a table that will allow you to go from Manufacturer to Make
--For example, Mercedes AG of Germany and Mercedes USA both roll up into Mercedes
--But they use different WMI Codes
create table MAX_VIN.DECODE.MANUFTOMAKE (
    MANUF_ID NUMBER(6),
    MAKE_NAME VARCHAR(50),
    MAKE_ID NUMBER(5)
    );

--We need a table that can decode the model year
-- The year 2001 is represented by the digit 1
-- The year 2020 is represented by the letter L
create table MAX_VIN.DECODE.MODELYEAR (
    MODYEARCODE	VARCHAR(1),
    MODYEARNAME NUMBER(4)
    );

--We need a table that can decode which plant at which 
--the vehicle was assembled
--You might have code "A" for Honda and code "A" for Ford
--so you need both the Make and the Plant Code to properly decode 
--the plant code
create table MAX_VIN.DECODE.MANUFPLANTS (
    MAKE_ID NUMBER(5),
    PLANTCODE VARCHAR(1),
    PLANTNAME VARCHAR(75)
    );
 
--We need to use a combination of both the Make and VDS 
--to decode many attributes including the engine, transmission, etc
create table MAX_VIN.DECODE.MMVDS (
    MAKE_ID	NUMBER(3),
    MODEL_ID NUMBER(6),
    MODEL_NAME VARCHAR(50),
    VDS	VARCHAR(5),
    DESC1 VARCHAR(25),
    DESC2 VARCHAR(25),
    DESC3 VARCHAR(50),
    DESC4 VARCHAR(25),
    DESC5 VARCHAR(25),
    BODYSTYLE VARCHAR(25),
    ENGINE VARCHAR(100),
    DRIVETYPE VARCHAR(50),
    TRANS VARCHAR(50),
    MPG	VARCHAR(25)
    );

-----------------------------------------------------------------------------------------------------

--  A File Format to Help Max Load the Data

--Create a file format and then load each of the 5 Lookup Tables
--You need a file format if you want to load the table
create file format MAX_VIN.DECODE.COMMA_SEP_HEADERROW 
    TYPE = 'CSV' 
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  
    TRIM_SPACE = TRUE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('\\N');

-----------------------------------------------------------------------------------------------------

-- Load the Tables and Check Out the Data

list @demo_db.public.like_a_window_into_an_s3_bucket/smew;
/*
smew/Maxs_MMVDS_Data.csv
smew/Maxs_ManufPlants_Data.csv
smew/Maxs_ManufToMake_Data.csv
smew/Maxs_ModelYear_Data.csv
smew/Maxs_WMIToManuf_data.csv
*/

copy into MAX_VIN.DECODE.WMITOMANUF
from @demo_db.public.like_a_window_into_an_s3_bucket
files = ('smew/Maxs_WMIToManuf_data.csv')
file_format =(format_name=MAX_VIN.DECODE.COMMA_SEP_HEADERROW);

copy into MAX_VIN.DECODE.MANUFTOMAKE
from @demo_db.public.like_a_window_into_an_s3_bucket
files = ('smew/Maxs_ManufToMake_Data.csv')
file_format =(format_name=MAX_VIN.DECODE.COMMA_SEP_HEADERROW);

copy into MAX_VIN.DECODE.MODELYEAR
from @demo_db.public.like_a_window_into_an_s3_bucket
files = ('smew/Maxs_ModelYear_Data.csv')
file_format =(format_name=MAX_VIN.DECODE.COMMA_SEP_HEADERROW);

copy into MAX_VIN.DECODE.MANUFPLANTS
from @demo_db.public.like_a_window_into_an_s3_bucket
files = ('smew/Maxs_ManufPlants_Data.csv')
file_format =(format_name=MAX_VIN.DECODE.COMMA_SEP_HEADERROW);

copy into MAX_VIN.DECODE.MMVDS
from @demo_db.public.like_a_window_into_an_s3_bucket
files = ('smew/Maxs_MMVDS_Data.csv')
file_format =(format_name=MAX_VIN.DECODE.COMMA_SEP_HEADERROW);

-----------------------------------------------------------------------------------------------------

-- Join the Decode Tables with the Table Lottie Provided

--Max has Lottie's VINventory table. Now he'll join his decode tables to the data
-- He'll create a select statement that ties each table into Lottie's VINS
-- Every time he adds a new table, he'll make sure he still has 298 rows

select *
from ACME_DETROIT.ADU.LOTSTOCK l-- he uses Lottie's data from the INBOUND SHARE 
join MAX_VIN.DECODE.MODELYEAR y -- and confirms he can join it with his own decode data
on l.modyearcode=y.modyearcode;

select *
from ACME_DETROIT.ADU.LOTSTOCK l -- he uses Lottie's data from the INBOUND SHARE 
join MAX_VIN.DECODE.WMITOMANUF w -- and confirms he can join it with his own decode data
on l.WMI=w.WMI;

--Add the next table (still 298?)
select *
from ACME_DETROIT.ADU.LOTSTOCK l -- he uses Lottie's data from the INBOUND SHARE 
join MAX_VIN.DECODE.WMITOMANUF w -- and confirms he can join it with his own decode data 
on l.WMI=w.WMI
join MAX_VIN.DECODE.MANUFTOMAKE m
on w.manuf_id=m.manuf_id;

--Until finally he has all 5 lookup tables added
--He can then remove the asterisk and start narrowing down the 
--fields to include in the final output
select 
    l.VIN, y.MODYEARNAME, m.MAKE_NAME, v.DESC1, v.DESC2, v.DESC3, BODYSTYLE, ENGINE
    DRIVETYPE, TRANS, MPG, MANUF_NAME, COUNTRY, VEHICLETYPE, PLANTNAME
from ACME_DETROIT.ADU.LOTSTOCK l -- he joins Lottie's data from the INBOUND SHARE 
join MAX_VIN.DECODE.WMITOMANUF w -- with all his data (he just tested)
    on l.WMI=w.WMI
join MAX_VIN.DECODE.MANUFTOMAKE m
    on w.manuf_id=m.manuf_id
join MAX_VIN.DECODE.MANUFPLANTS p
    on l.plantcode=p.plantcode
    and m.make_id=p.make_id
join MAX_VIN.DECODE.MMVDS v
    on v.vds=l.vds 
    and v.make_id = m.make_id
join MAX_VIN.DECODE.MODELYEAR y
    on l.modyearcode=y.modyearcode;

-----------------------------------------------------------------------------------------------------

-- Create a View

-- Once the select statement looks good (above), Max lays a view on top of it
-- this will make it easier to use in a Stored procedure

use role accountadmin;
create or replace database MAX_OUTGOING; --this new database will be used for his OUTBOUND SHARE
create or replace schema MAX_OUTGOING.FOR_ACME; --this schema he creates especially for ACME

-- This is a live view of the data Lottie and Caden Need!
create or REPLACE SECURE VIEW MAX_OUTGOING.FOR_ACME.LOTSTOCKENHANCED as 
(
select 
    l.VIN,
    y.MODYEARNAME,
    m.MAKE_NAME,
    v.DESC1,
    v.DESC2,
    v.DESC3,
    BODYSTYLE,
    ENGINE,
    DRIVETYPE,
    TRANS,
    MPG,
    EXTERIOR,
    INTERIOR,
    MANUF_NAME,
    COUNTRY,
    VEHICLETYPE,
    PLANTNAME
from ACME_DETROIT.ADU.LOTSTOCK l
join MAX_VIN.DECODE.WMITOMANUF w
    on l.WMI=w.WMI
join MAX_VIN.DECODE.MANUFTOMAKE m
    on w.manuf_id=m.manuf_id
join MAX_VIN.DECODE.MANUFPLANTS p
    on l.plantcode=p.plantcode
    and m.make_id=p.make_id
join MAX_VIN.DECODE.MMVDS v
    on v.vds=l.vds and v.make_id = m.make_id
join MAX_VIN.DECODE.MODELYEAR y
    on l.modyearcode=y.modyearcode
);

-----------------------------------------------------------------------------------------------------

-- Set Up the Shareback Table

-- Even though it would be nice to share the view back to Lottie, 
-- You can't share a share so we have to make a copy of the data to share back

create or replace table MAX_OUTGOING.FOR_ACME.LOTSTOCKRETURN (
    VIN VARCHAR(17),
    MODYEARNAME	NUMBER(4),
    MAKE_NAME VARCHAR(50),
    DESC1 VARCHAR(50),
    DESC2 VARCHAR(50),
    DESC3 VARCHAR(50),
    BODYSTYLE VARCHAR(25),
    ENGINE VARCHAR(100),
    DRIVETYPE VARCHAR(50),
    TRANS VARCHAR(50),
    MPG VARCHAR(25),
    EXTERIOR VARCHAR(50),
    INTERIOR VARCHAR(50),
    MANUF_NAME VARCHAR(50),
    COUNTRY VARCHAR(50),
    VEHICLETYPE	VARCHAR(50),
    PLANTNAME VARCHAR(75)
    );

-----------------------------------------------------------------------------------------------------

-- Create a Stored Procedure to Load Lottie's Enhanced Data into a Shareable Table

--=============================STORED PROCEDURE====================================
-- Create a stored proc that will dump and reload the vinhanced table 
-- using the view that combines Lottie's data with Max's. You don't actually need 
-- two sets of variables but using two might help make it clearer for some people. 

create or replace procedure lotstockupdate_sp()
  returns string not null
  language javascript
  as
  $$
    var my_sql_command1 = "truncate table max_outgoing.for_acme.lotstockreturn;";
    var statement1 = snowflake.createStatement( {sqlText: my_sql_command1} );
    var result_set1 = statement1.execute();
    
    var my_sql_command2 ="insert into max_outgoing.for_acme.lotstockreturn ";    
    my_sql_command2 += "select VIN, MODYEARNAME, MAKE_NAME, DESC1, DESC2, DESC3, BODYSTYLE";
    my_sql_command2 += ",ENGINE, DRIVETYPE, TRANS, MPG, EXTERIOR, INTERIOR, MANUF_NAME, COUNTRY, VEHICLETYPE, PLANTNAME";
    my_sql_command2 += " from max_outgoing.for_acme.lotstockenhanced;";

    var statement2 = snowflake.createStatement( {sqlText: my_sql_command2} ); 
    var result_set2 = statement2.execute(); 
    return my_sql_command2; 
   $$; 
   
   --View your Stored Procedure 
   show procedures in account; 
   desc procedure lotstockupdate_sp(); 
-----------------------------------------------------------------------------------------------------

-- Create a Task that Runs Your Stored Procedure and Manage It

--==========SCHEDULED TASK============================================== 
-- Create a task that calls the stored procedure every hour 
-- so that Lottie sees updates at least every hour

use role ACCOUNTADMIN;
grant execute task on account to role sysadmin;

create or replace task acme_return_update
  warehouse = compute_wh
  schedule = '1 minute'
as
  call lotstockupdate_sp();

--if you need to see who owns the task
show grants on task acme_return_update;
 
--Look at the task you just created to make sure it turned out okay
show tasks;
desc task acme_return_update;
 
--if you task has a state of "suspended" run this to get it going
alter task acme_return_update resume;  
 
--Check back 5 mins later to make sure your task has been running
--You will not be able to see your task on the Query History Tab
select *
  from table(information_schema.task_history())
  order by scheduled_time;
  
-----------------------------------------------------------------------------------------------------

-- Pause the Task!

--=== CHECK ON AND SUSPEND THE SCHEDULED TASK ========================== 
show tasks in account;
desc task acme_return_update;
 
alter task acme_return_update suspend;  
 
--Check back 5 mins later to make sure your task is NOT running
desc task acme_return_update;