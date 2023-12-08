---- PREPARATION ----

-- Create database
CREATE OR REPLACE DATABASE DATA_S;

-- Create stage
CREATE OR REPLACE STAGE aws_stage
    URL = 's3://bucketsnowflakes3';

-- List files in stage
LIST @aws_stage;

-- Create table
CREATE OR REPLACE TABLE ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT NUMBER(38,0),
    PROFIT NUMBER(38,0),
    QUANTITY NUMBER(38,0),
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

-- Load data using copy command
COPY INTO ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER = 1)
    PATTERN ='.*OrderDetails.*';

SELECT * FROM ORDERS;

---- Why Secure view is recommended? ----

CREATE OR REPLACE VIEW ORDERS_VIEW AS
    SELECT 
        ORDER_ID,
        AMOUNT,
        QUANTITY
    FROM ORDERS
    WHERE CATEGORY != 'Furniture';

SHOW VIEWS LIKE '%ORDER%';

-- Create Secure View
CREATE OR REPLACE SECURE VIEW ORDERS_VIEW_SECURE AS
    SELECT 
        ORDER_ID,
        AMOUNT,
        QUANTITY
    FROM ORDERS
    WHERE CATEGORY != 'Furniture';

-- STEP 1: Create a share object

-- You need the ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create Share
CREATE OR REPLACE SHARE ORDERS_SHARE;

-- STEP 2: Setup Grants 

-- Grant usage on database
GRANT USAGE ON DATABASE DATA_S TO SHARE ORDERS_SHARE; 

-- Grant usage on schema
GRANT USAGE ON SCHEMA DATA_S.PUBLIC TO SHARE ORDERS_SHARE; 

-- Grant SELECT on table (not a best practice)
-- GRANT SELECT ON TABLE DATA_S.PUBLIC.ORDERS TO SHARE ORDERS_SHARE;

-- Grant select on view
GRANT SELECT ON VIEW  DATA_S.PUBLIC.ORDERS_VIEW TO SHARE ORDERS_SHARE; 
-- You can't share normal view with snowflake (it's shows DDL command for that view)
GRANT SELECT ON VIEW  DATA_S.PUBLIC.ORDERS_VIEW_SECURE TO SHARE ORDERS_SHARE;

-- Validate Grants
SHOW GRANTS TO SHARE ORDERS_SHARE;

-- Create Reader Account --
CREATE MANAGED ACCOUNT reader_account
    ADMIN_NAME = reader_account_admin,
    ADMIN_PASSWORD = 'Password@123',
    TYPE = READER;

--- To drop the reader account: 
-- DROP MANAGED ACCOUNT reader_account;

-- Show managed accounts
SHOW MANAGED ACCOUNTS;
-- You get an URL, so yo u can login to reader_account.
-- You can get an identifier from the URL of the reader account, that will be used as consumer_locator.

-- Share the data
ALTER SHARE ORDERS_SHARE 
    ADD ACCOUNT = <consumer_locator>;

-- Sharing to a lower edition
ALTER SHARE ORDERS_SHARE 
    ADD ACCOUNT =  <consumer_locator>
    SHARE_RESTRICTIONS = FALSE;
-- If we are using Business Critical edition and we want to share data to lower edition ...

---- By using reader account ----

-- STEP 4: Create database from share

-- Use ACCOUNTADMIN role on consumer account
USE ROLE ACCOUNTADMIN;

-- Show all shares (consumer & producers)
SHOW SHARES;

-- See details on share
DESC SHARE <consumer_account>.ORDERS_SHARE;

-- Create a database in consumer account using the share
CREATE DATABASE DATA_SHARE_DB FROM SHARE <account_producer>.ORDERS_SHARE;

-- Validate table access
SELECT * FROM  DATA_SHARE_DB.PUBLIC.ORDERS;

-- Setup virtual warehouse
-- Compute cost must be paid by provider account with reader account
CREATE WAREHOUSE READ_WH WITH
    WAREHOUSE_SIZE='X-SMALL'
    AUTO_SUSPEND = 180
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- Validate table access
SELECT * FROM  DATA_SHARE_DB.PUBLIC.ORDERS;
