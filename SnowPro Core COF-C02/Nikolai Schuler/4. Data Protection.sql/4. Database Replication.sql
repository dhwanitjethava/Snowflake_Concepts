-- Show Organisation account information
SHOW ORGANIZATION ACCOUNTs;

-- Enable replication for each source and target account in your organization
USE ROLE ORGADMIN;
SELECT SYSTEM$GLOBAL_ACCOUNT_SET_PARAMETER ('<organization_name>.<account_name>',
                                            'ENABLE_ACCOUNT_DATABASE_REPLICATION', 'TRUE');

-- Promote a Local database to Primary Database
USE ROLE ACCOUNTADMIN;
ALTER DATABASE <primary_database_name> ENABLE REPLICATION TO ACCOUNTS <account_name1> , <account_name2>;
-- Accounts must be within same organisation with Primary Database

-- Create replica in consumer account
CREATE DATABASE <database_name> AS REPLICA OF <primary_database_name>;
-- database_name should be same as primary_database_name because it is replica of the primary database
-- that is on consumer account.

-- Refresh database
ALTER DATABASE <database_name> REFRESH;
