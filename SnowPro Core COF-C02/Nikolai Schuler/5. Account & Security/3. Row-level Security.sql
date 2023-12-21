---- ROW ACCESS POLICIES ----

-- Table of interest
SELECT * FROM DATA_S.PUBLIC.ORDERS;

-- Thinking of policy based on category
SELECT DISTINCT CATEGORY FROM DATA_S.PUBLIC.ORDERS;

USE DATA_S.PUBLIC;
USE ROLE ACCOUNTADMIN;

-- Set up role
CREATE OR REPLACE ROLE home_manager;

GRANT USAGE ON DATABASE data_s TO ROLE home_manager;
GRANT USAGE ON SCHEMA data_s.public TO ROLE home_manager;   -- wouldn't be necessary since it is public schema
GRANT SELECT ON TABLE data_s.public.orders TO ROLE home_manager;

GRANT USAGE ON warehouse compute_wh TO ROLE home_manager;

GRANT ROLE home_manager TO USER dhwanit;

-- Table can be queried and all rows are visible
USE ROLE home_manager;     
SELECT * FROM ORDERS;

-- Policy that role can see everything
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE ROW ACCESS POLICY category_policy
AS (category varchar) RETURNS BOOLEAN ->
    CASE WHEN 'HOME_MANAGER' = current_role()  AND 'Furniture' = category THEN TRUE
    ELSE FALSE
    END;

-- Roles are in ALL CAPS
SELECT current_role();
      
DESC ROW ACCESS POLICY category_policy;

-- Test policy (not working)      
USE ROLE home_manager;  
SELECT * FROM ORDERS;

-- Add policy to table
USE ROLE ACCOUNTADMIN;
ALTER TABLE data_s.public.orders ADD ROW ACCESS POLICY category_policy ON (category);

-- Remove policy from table
ALTER TABLE data_s.public.orders DROP ROW ACCESS POLICY category_policy;
ALTER TABLE data_s.public.orders DROP ALL ROW ACCESS POLICIES;

-- Test policy (working)      
USE ROLE home_manager;     
SELECT * FROM ORDERS;

SELECT current_user();

-- Alter policy
USE ROLE ACCOUNTADMIN;
ALTER ROW ACCESS POLICY category_policy
SET BODY -> 
    CASE WHEN 'DHWANIT' = current_user() and 'Furniture' = category THEN TRUE
    WHEN 'ELECTRIC_MANAGER' = current_role() and 'Electronics' = category THEN TRUE
    ELSE FALSE
    END;