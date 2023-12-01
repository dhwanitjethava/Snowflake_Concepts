-- Dynamic Data Masking

-- DDM: column-level security feature that uses masking policies to selectively mask sensitive data
--      in table and view columns at query time.
--      This means the underlying data is not altered in the database, but rather masked as it is retrieved.

-----------------------------------------------------------------------------------------------------------------------

-- Create masking policy

-- Set up Roles
CREATE OR REPLACE ROLE <ANALYST_MASKED>;
CREATE OR REPLACE ROLE <ANALYST_FULL>;

-- Grant SELECT on table to Roles
GRANT SELECT ON TABLE <table_name> TO ROLE <ANALYST_MASKED>;
GRANT SELECT ON TABLE <table_name> TO ROLE <ANALYST_FULL>;

-- Grant USAGE on schema to Roles
GRANT USAGE ON SCHEMA <schema_name> TO ROLE <ANALYST_MASKED>;
GRANT USAGE ON SCHEMA <schema_name> TO ROLE <ANALYST_FULL>;

-- Grant warehouse access to Roles
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE <ANALYST_MASKED>;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE <ANALYST_FULL>;

-- Assign Roles to User
GRANT ROLE <ANALYST_MASKED> TO USER DHWANITJETHAVA;
GRANT ROLE <ANALYST_FULL> TO USER DHWANITJETHAVA;

-----------------------------------------------------------------------------------------------------------------------

-- Set up masking policy
CREATE OR REPLACE MASKING POLICY <policy_name> 
AS (val <datatype_original_column>) RETURNS <datatype_masking_column> ->       -- Both datatypes must be same
    CASE
        WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val     
        ELSE '##-###-##'
    END;

-- Apply policy on a specific column 
ALTER TABLE IF EXISTS <table_name> MODIFY COLUMN <column_name> 
SET MASKING POLICY <policy_name>;

-- Validating policies
USE ROLE ANALYST_FULL;
SELECT * FROM <table_name>;

USE ROLE ANALYST_MASKED;
SELECT * FROM <table_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Unset & Replace masking policy

-- List and Describe policies
DESC MASKING POLICY <policy_name>;
SHOW MASKING POLICIES;

-- Show columns with applied policies
SELECT * FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(POLICY_NAME => '<policy_name>'));

-- Remove policy before replacing/dropping
ALTER TABLE IF EXISTS <table_name> MODIFY COLUMN <column_name>
UNSET MASKING POLICY;

-----------------------------------------------------------------------------------------------------------------------

-- Masking policy with string concatenation
CREATE OR REPLACE MASKING POLICY <policy_name> 
AS (val <datatype_original_column>) RETURNS <datatype_masking_column> ->       -- Both datatypes must be same
    CASE
        WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val     
        ELSE CONCAT(LEFT(val,2), '********')
    END;

-----------------------------------------------------------------------------------------------------------------------

-- Alter existing policy 

USE ROLE ACCOUNTADMIN;

-- Alter policy
ALTER MASKING POLICY <policy_name> SET BODY ->
CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_FULL', 'ACCOUNTADMIN') THEN val     
    ELSE '**-**-**'
END;

-----------------------------------------------------------------------------------------------------------------------

-- Examples

-- 1. Masking policy to unmask email domain in email address

CREATE OR REPLACE MASKING POLICY <policy_name>
AS (val VARCHAR) RETURNS VARCHAR ->
CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_FULL', THEN val
    WHEN CURRENT_ROLE() IN ('ANALYST_MASKED') THEN REGEXP_REPLACE(val,'.+\@','*****@')
    ELSE '********'
END;

-- 2. Masking policy to return hash value of the column value

CREATE OR REPLACE MASKING POLICY <policy_name>
AS (val VARCHAR) RETURNS VARCHAR ->
CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_FULL', THEN val
    ELSE sha2(val)                                   -- returns hash of the column value
END;

-- 3. Masking policy to return masking date datatype

CREATE OR REPLACE MASKING POLICY <policy_name>
AS (val DATE) RETURNS DATE ->
CASE
    WHEN CURRENT_ROLE() IN ('ANALYST_FULL', THEN val
    ELSE date_from_parts(0001, 01, 01)::DATE         -- returns 0001-01-01 00:00:00.000
END;

-----------------------------------------------------------------------------------------------------------------------