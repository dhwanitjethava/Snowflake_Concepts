-- Set Up the ACME Database and Schemas
use role SYSADMIN;
--Caden set up a new database (and you will, too)
create database ACME;

--get rid of the public schema - too generic
drop schema PUBLIC;

--When creating shares it is best to have multiple schemas
create schema ACME.SALES;
create schema ACME.STOCK;
create schema ACME.ADU; --this is the schema they'll use to share to ADU, Max's company

----------------------------------------------------------------------------------------------

-- Lotstock Table and View
use role SYSADMIN;

-- Lottie's team will enter new stock into this table when inventory is received
-- the Date_Sold and Customer_Id will be null until the car is sold
create or replace table ACME.STOCK.LOTSTOCK (
    VIN VARCHAR(17),
    EXTERIOR VARCHAR(50),	
    INTERIOR VARCHAR(50),
    DATE_SOLD DATE,
    CUSTOMER_ID NUMBER(20)
    );

-- This secure view breaks the VIN into digestible components
-- This view only shares unsold cars because the unsold cars are the ones that need to be enhanced
create or replace secure view ACME.ADU.LOTSTOCK as (
select VIN,
    LEFT(VIN,3) as WMI,
    SUBSTR(VIN,4,5) as VDS,
    SUBSTR(VIN,10,1) as MODYEARCODE,
    SUBSTR(VIN,11,1) as PLANTCODE,
    EXTERIOR,
    INTERIOR
from ACME.STOCK.LOTSTOCK
where DATE_SOLD is NULL
);

----------------------------------------------------------------------------------------------

-- A File Format to Help Caden Load the Data

create file format ACME.STOCK.COMMA_SEP_HEADERROW 
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

----------------------------------------------------------------------------------------------

-- Load the table and check out the data

-- Use a COPY INTO to load the data
-- the file is named Lotties_LotStock_Data.csv
-- create stage first
create or replace stage demo_db.public.like_a_window_into_an_s3_bucket 
	url = 's3://uni-lab-files';
    
copy into acme.stock.lotstock
from @demo_db.public.like_a_window_into_an_s3_bucket
files = ('smew/Lotties_LotStock_Data.csv')
file_format =(format_name=ACME.STOCK.COMMA_SEP_HEADERROW);


-- After loading your base table is no longer empty
-- it should now have 300 rows
select * from acme.stock.lotstock;

--the View will show just 298 rows because the view only shows
--rows where the date_sold is null
select * from acme.adu.lotstock;

----------------------------------------------------------------------------------------------
-- You don't need the ACME database anymore
alter database ACME rename to back_when_i_pretended_i_was_Caden;
