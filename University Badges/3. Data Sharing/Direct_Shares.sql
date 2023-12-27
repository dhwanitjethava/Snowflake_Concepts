-- Setting the Sample Share Name Back to the Original Name
alter database cool_sample_stuff
rename to snowflake_sample_data;

-- Try this and see the ERROR
drop database SNOWFLAKE;

-- Granting Imported Privileges
grant IMPORTED PRIVILEGES on database SNOWFLAKE_SAMPLE_DATA to role SYSADMIN;

---------------------------------------------------------------------------------------------
-- Use Select Statements to Look at Sample Data

-- Check the range of values in the Market Segment Column
select distinct c_mktsegment from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

-- Find out which Market Segments have the most customers
select c_mktsegment, COUNT(*) from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER
group by c_mktsegment
order by COUNT(*);

---------------------------------------------------------------------------------------------
-- Join and Aggregate Shared Data

-- Nations Table
select N_NATIONKEY, N_NAME, N_REGIONKEY from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

-- Regions Table
select R_REGIONKEY, R_NAME from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- Join the Tables and Sort
select R_NAME as Region, N_NAME as Nation from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
on N_REGIONKEY = R_REGIONKEY
order by R_NAME, N_NAME asc;

--Group and Count Rows Per Region
select R_NAME as Region, count(N_NAME) as NUM_COUNTRIES
from SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION 
join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION 
on N_REGIONKEY = R_REGIONKEY
group by R_NAME;

---------------------------------------------------------------------------------------------
-- Joining Local Data With Shared Data

-- Create a Local Database INTL_DB
use role SYSADMIN;
create database INTL_DB;
use schema INTL_DB.PUBLIC;

-- Create a Warehouse for Loading INTL_DB
create warehouse INTL_WH with WAREHOUSE_SIZE = 'XSMALL' 
    warehouse_type = 'STANDARD' 
    auto_suspend = 600 
    auto_resume = TRUE;

use warehouse INTL_WH;

-- Create Table INT_STDS_ORG_3661
create or replace table INTL_DB.PUBLIC.INT_STDS_ORG_3661 (
    ISO_COUNTRY_NAME varchar(100), 
    COUNTRY_NAME_OFFICIAL varchar(200), 
    SOVEREIGNTY varchar(40), 
    ALPHA_CODE_2DIGIT varchar(2), 
    ALPHA_CODE_3DIGIT varchar(3), 
    NUMERIC_COUNTRY_CODE integer,
    ISO_SUBDIVISION varchar(15), 
    INTERNET_DOMAIN_CODE varchar(10)
    );

-- Create a File Format to Load the Table
create or replace file format INTL_DB.PUBLIC.PIPE_DBLQUOTE_HEADER_CR 
  TYPE = 'CSV' 
  COMPRESSION = 'AUTO' 
  FIELD_DELIMITER = '|' 
  RECORD_DELIMITER = '\r' 
  SKIP_HEADER = 1 
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042' 
  TRIM_SPACE = FALSE 
  ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
  ESCAPE = 'NONE' 
  ESCAPE_UNENCLOSED_FIELD = '\134'
  DATE_FORMAT = 'AUTO' 
  TIMESTAMP_FORMAT = 'AUTO' 
  NULL_IF = ('\\N');

-- Load the ISO Table Using Your File Format
-- Firstly create internal stage for loading file from S3
create or replace stage intl_db.public.like_a_window_into_an_s3_bucket 
	url = 's3://uni-lab-files/smew/';

-- Use COPY INTO command for load data into table
copy into INTL_DB.PUBLIC.INT_STDS_ORG_3661 from @intl_db.public.like_a_window_into_an_s3_bucket
    files = ('ISO_Countries_UTF8_pipe.csv')
    file_format = (format_name='PIPE_DBLQUOTE_HEADER_CR');

-- Check That You Created and Loaded the Table Properly
SELECT count(*) as FOUND, '249' as EXPECTED FROM INTL_DB.PUBLIC.INT_STDS_ORG_3661;

-- Test Whether You Set Up Your Table in the Right Place with the Right Name
-- If it right, we should get back the count of 1. 
select count(*) as OBJECTS_FOUND
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3661';

-- How to Test That You Loaded the Expected Number of Rows
select row_count
from INTL_DB.INFORMATION_SCHEMA.TABLES 
where table_schema='PUBLIC'
and table_name= 'INT_STDS_ORG_3661';

-- Join Local Data with Shared Data
select iso_country_name, country_name_official, alpha_code_2digit, r_name as region
from INTL_DB.PUBLIC.INT_STDS_ORG_3661 i
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
on upper(i.iso_country_name)=n.n_name
left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
on n_regionkey = r_regionkey;

-- Convert the Select Statement into a View
create view NATIONS_SAMPLE_PLUS_ISO (iso_country_name,country_name_official,alpha_code_2digit,region)
as select iso_country_name, country_name_official, alpha_code_2digit, r_name as region
    from INTL_DB.PUBLIC.INT_STDS_ORG_3661 i
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION n
    on upper(i.iso_country_name)=n.n_name
    left join SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION r
    on n_regionkey = r_regionkey;

-- A view can capture a complex select statement and make it simple to run repeatedly
-- Run a SELECT on the View You Created
select * from INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO;

-- Create Table Currencies
create table INTL_DB.PUBLIC.CURRENCIES (
    CURRENCY_ID INTEGER, 
    CURRENCY_CHAR_CODE varchar(3), 
    CURRENCY_SYMBOL varchar(4), 
    CURRENCY_DIGITAL_CODE varchar(3), 
    CURRENCY_DIGITAL_NAME varchar(30)
    )
    comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

-- Use COPY INTO command for load data into table Currencies
copy into INTL_DB.PUBLIC.CURRENCIES from @intl_db.public.like_a_window_into_an_s3_bucket
    files = ('currencies.csv')
    file_format = (format_name='CSV_COMMA_LF_HEADER');

-- Create Table Country to Currency
create table INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE (
    COUNTRY_CHAR_CODE Varchar(3), 
    COUNTRY_NUMERIC_CODE INTEGER, 
    COUNTRY_NAME Varchar(100), 
    CURRENCY_NAME Varchar(100), 
    CURRENCY_CHAR_CODE Varchar(3), 
    CURRENCY_NUMERIC_CODE INTEGER
    ) 
    comment = 'Many to many code lookup table';

-- Use COPY INTO command for load data into table Currencies
copy into INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE from @intl_db.public.like_a_window_into_an_s3_bucket
    files = ('country_code_to_currency_code.csv')
    file_format = (format_name='CSV_COMMA_LF_HEADER');

-- Create a File Format to Process files with Commas, Linefeeds and a Header Row
create file format INTL_DB.PUBLIC.CSV_COMMA_LF_HEADER
    TYPE = 'CSV'
    COMPRESSION = 'AUTO' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' 
    SKIP_HEADER = 1 
    FIELD_OPTIONALLY_ENCLOSED_BY = 'NONE' 
    TRIM_SPACE = FALSE 
    ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
    ESCAPE = 'NONE' 
    ESCAPE_UNENCLOSED_FIELD = '\134' 
    DATE_FORMAT = 'AUTO' 
    TIMESTAMP_FORMAT = 'AUTO' 
    NULL_IF = ('\\N');

-- Challange : Create a View - Done
select * from INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE
limit 5;

create view SIMPLE_CURRENCY (CTY_CODE, CUR_CODE)
as select COUNTRY_CHAR_CODE as CTY_CODE, CURRENCY_CHAR_CODE as CUR_CODE
    from INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE;

select * from SIMPLE_CURRENCY;

---------------------------------------------------------------------------------------------
-- Making Changes to Existing Outbound Shares

-- Convert "Regular" Views to "Secure" Views
alter view INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO set SECURE; 
alter view INTL_DB.PUBLIC.SIMPLE_CURRENCY set SECURE;