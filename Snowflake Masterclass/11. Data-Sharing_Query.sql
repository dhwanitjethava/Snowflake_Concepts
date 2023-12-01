-- Data sharing w/o actual copy of the data & upto date
-- Shared data can be consumed by the own compute resources
-- Non-Snowflake-Users can also access through a reader account
-- Note: Non-Snowflake-User - Independent instance with own URL & own compute resources.
-----------------------------------------------------------------------------------------------------------------------

-- Method 1: With SQL Statements

-- Switch role to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create share object
CREATE OR REPLACE SHARE <share_object_name>;

-- Setup Grants for above Share Object

-- Grant usage on database
GRANT USAGE ON DATABASE <database_name> TO SHARE <share_object_name>;

-- Grant usage on schema
GRANT USAGE ON SCHEMA <schema_name> TO SHARE <share_object_name>;

-- Grant SELECT on table
GRANT SELECT ON TABLE <table_name> TO SHARE <share_object_name>;

-- Validate Grants
SHOW GRANTS TO SHARE <share_object_name>;

-- Add consumer account with unique account number
ALTER SHARE <share_object_name> ADD ACCOUNT = <consumer_account>;

-- On consumer account --

-- Show all shares (INBOUND & OUTBOUND)
SHOW SHARES;

-- See details on share
DESC SHARE <producer_account>.<share_object_name>;

-- Create a database in consumer account using share object
CREATE OR REPLACE DATABASE <database_name> FROM SHARE <producer_account>.<share_object_name>;

-- Validate table access on consumer account
SELECT * FROM <table_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Method 2: With Snowflake UI

-----------------------------------------------------------------------------------------------------------------------

-- Reader Account

-- Switch role to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Create Reader Account
CREATE MANAGED ACCOUNT <reader_account_name>
ADMIN_NAME = <reader_admin_name>,
ADMIN_PASSWORD = 'set_password',
TYPE = READER; 
-- Result gives you JSON file having 'accountName' and 'loginUrl'.

-- Show accounts
SHOW MANAGED ACCOUNTS;

-- Share the data
ALTER SHARE <share_object_name>
ADD ACCOUNT = <consumer_account>;

ALTER SHARE <share_object_name>
ADD ACCOUNT = <consumer_account>
SHARE_RESTRICTIONS = FALSE;  -- For Business Critical Account

-- On Reader's account --

-- Switch role to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Show all shares (INBOUND & OUTBOUND)
SHOW SHARES;

-- See details on share
DESC SHARE <producer_account>.<share_object_name>;

-- Setup Virtual Warehouse
CREATE WAREHOUSE <warehouse_name> WITH
WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 180
AUTO_RESUME = TRUE
INITIALLY_SUSPENDED = TRUE;

-- Create a database in consumer account using share object
CREATE OR REPLACE DATABASE <database_name> FROM SHARE <producer_account>.<share_object_name>;

-- Validate table access on consumer account
SELECT * FROM <table_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Set up users for Share in Reader account

-- Create User
CREATE USER <username> PASSWORD = '<set_password>';

-- Grant usage on warehouse
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE PUBLIC; --Public role for all users

-- Granting privileges on a Shared Database for other users
GRANT IMPORTED PRIVILEGES ON DATABASE <database_name> TO ROLE PUBLIC;

-----------------------------------------------------------------------------------------------------------------------

-- Share complete Schema or Database --

-- Create Share Object
CREATE OR REPLACE SHARE <share_object_name>;

-- Grant usage on database & schema
GRANT USAGE ON DATABASE <database_name> TO SHARE <share_object_name>;
GRANT USAGE ON SCHEMA <schema_name> TO SHARE <share_object_name>;

-- Grant SELECT ON ALL tables
GRANT SELECT ON ALL TABLES IN DATABASE <database_name> TO SHARE <share_object_name>;
GRANT SELECT ON ALL TABLES IN SCHEMA <schema_name> TO SHARE <share_object_name>;

-- Add account to Share object
ALTER SHARE <share_object_name>
ADD ACCOUNT = <consumer_account>;

-- On Reader's account --

-- Switch role to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Show all shares (INBOUND & OUTBOUND)
SHOW SHARES;

-- See details on share
DESC SHARE <producer_account>.<share_object_name>;

-- Create a database in consumer account using share object
CREATE OR REPLACE DATABASE <database_name> FROM SHARE <producer_account>.<share_object_name>;

-- Validate table access on consumer account
SELECT * FROM <table_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Create NORMAL VIEW

CREATE OR REPLACE VIEW <view_name> AS
    SELECT column1, column3, column4, column5 FROM <table_name>
    WHERE <condition>;

-- Grant usage & SELECT
GRANT USAGE ON DATABASE <database_name> TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA <schema_name> TO TO ROLE PUBLIC;
GRANT SELECT ON TABLE <table_name> TO ROLE PUBLIC;
GRANT SELECT ON VIEW <view_name> TO ROLE PUBLIC;

-- Secure views are similar to normal views,
-- but they will not expose underlying view definitions (tables used in SQL) for unauthorized Users and Roles. 
-- Implementing data level security is easy when creating secure views.
-- In secure view, DDL statement of view is not visible to shared account or role.

-- Create SECURE VIEW

CREATE OR REPLACE SECURE VIEW <secure_view_name> AS
    SELECT column1, column3, column4, column5 FROM <table_name>
    WHERE <condition>;

-- Grant SELECT
GRANT SELECT ON VIEW <secure_view_name> TO ROLE PUBLIC;

-- Grant usage on dabase & schema
GRANT USAGE ON DATABASE <database_name> TO SHARE <share_object_name>;
GRANT USAGE ON SCHEMA <schema_name> TO SHARE <share_object_name>;

-- Grant SELECT on VIEW
GRANT SELECT ON VIEW <view_name> TO SHARE <share_object_name>;
  -- Error : VIEW can only be shared if it is created as a SECURE VIEW.
  -- Debug : Marked SECURE using query - ALTER VIEW <view_name> SET SECURE
GRANT SELECT ON VIEW <secure_view_name> TO SHARE <share_object_name>;

-- Add account to share
ALTER SHARE <share_object_name>
ADD ACCOUNT = <consumer_account>;

-- On Reader's account --

-- Switch role to ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Show all shares (INBOUND & OUTBOUND)
SHOW SHARES;

-- See details on share
DESC SHARE <producer_account>.<share_object_name>;

-- Create a database in consumer account using share object
CREATE OR REPLACE DATABASE <database_name> FROM SHARE <producer_account>.<share_object_name>;

-- Validate table access on consumer account
SELECT * FROM <secure_view_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Note: Only SECURE views can be shared to not be able to retrieve any further information beyond the shared data.
-- Note: A dedicated Virtual Warehouse of the provider account has to be set up to use compute on Reader account.