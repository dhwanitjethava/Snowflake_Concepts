-- Access Control
-- - Who can access and perform operations on objects in Snowflake.
-- - Two aspects of access control combined.
--   1. Discretionary Access Control (DAC)
--      Each object has an owner who can grant access to that object.
--   2. Role-based Access Control (RBAC)
--      Access privileges are assigned to roles, which are in turn assigned to users.
-- - Every object owned by a single role (multiple users).
-- - Owner (role) has all privileges per default.
-- - USER : People or systems
-- - ROLE : Entity to which privileges are granted (role hierarchy)
-- - PRIVILEGE : Level of access to an object (SELECT, DROP, CREATE etc.)

-----------------------------------------------------------------------------------------------------------------------

-- ROLE
-- - System defined roles : ACCOUNTADMIN > SECURITYADMIN = SYSADMIN > USERADMIN > PUBLIC

-- - ACCOUNTADMIN:
--   - Granted only to limited number of users
--   - Top-level role in the system
--   - Manage & view all objects
--   - All configurations on account level
--   - Account operation (Create reader account, billing information, etc)
--   - First user will have this role assigned
--   - Initial setup and managing account level objects
--   - BEST PRACTICES: Very controlled assignment strongly recommended!
--   - BEST PRACTICES: Multi-factor authentication
--   - BEST PRACTICES: At least two users should be assigned to that role
--   - BEST PRACTICES: Avoid creating objects with that role unless you have to

-- - SECURITYADMIN:
--   - USERADMIN role is granted to SECURITYADMIN
--   - Manage Users & Role
--   - Manage any object grant globally
--   - Access to Account Admin tab with limited access
--   - Create and manage users and roles
--   - Grant and revoke privileges to roles

-- - SYSADMIN: 
--   - Create and manage objects such as WH, databases, tables, etc.
--   - Custom roles should be assigned to the SYSADMIN role as the parent
--   - Ability to grant privileges on warehouses, databases, and other objects to the custom roles
--   - Customize roles to our needs & create own hierarchies
--   - Custom roles are usually created by SECURITYADMIN and assigned to SYSADMIN role
--   - Recommended that all custom roles are assigned to SYSADMIN role

-- - USERADMIN: 
--   - Dedicated to user & role management only
--   - Can create users and roles only
--   - Not for granting privileges (only the one that is owns)

-- - PUBLIC:
--   - Least privileged role
--   - Every user is automatically assigned to this role
--   - Can own objects - These objects are then available to everyone

-----------------------------------------------------------------------------------------------------------------------

-- ACCOUNTADMIN Role

-- Create User and Grant Role
CREATE USER <user_name> PASSWORD = '<password>'
DEFAULT_ROLE = ACCOUNTADMIN
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE ACCOUNTADMIN TO USER <user_name>;

-----------------------------------------------------------------------------------------------------------------------

-- SECURITYADMIN Role

-- Create User and Role
CREATE ROLE <role_name_admin>;
CREATE ROLE <role_name_user>;

-- Create hierarchy - role_name_admin > role_name_user
GRANT ROLE <role_name_user> TO <role_name_admin>;

-- BEST PRACTICES: Assign role to SYSADMIN
GRANT ROLE <role_name_admin> TO ROLE SYSADMIN;

-- Create Role for user
CREATE USER <user_name> PASSWORD = '<password>'
DEFAULT_ROLE = <role_name_user>
MUST_CHANGE_PASSWORD = TRUE;

GRANT ROLE <role_name_user> TO USER <user_name>;

-----------------------------------------------------------------------------------------------------------------------

-- SYSADMIN Role

-- Create a WH of size X-SMALL
CREATE WAREHOUSE <warehouse_name> WITH
WAREHOUSE_SIZE = 'X-SMALL'
AUTO_SUSPEND = 300
AUTO_RESUME = TRUE;

-- Grant usage warehouse to role public
GRANT USAGE ON <warehouse_name> TO ROLE PUBLIC;

-- Create a database accessible to everyone
CREATE OR REPLACE DATABASE <database_name1>;
GRANT USAGE ON DATABASE <database_name1> TO ROLE PUBLIC;

-- Create another database that is specific for Admin Role
CREATE OR REPLACE DATABASE <database_name2>;
GRANT OWNERSHIP ON DATABASE <database_name2> TO ROLE <role_name_admin>;
GRANT OWNERSHIP ON SCHEMA <database_name2.public> TO ROLE <role_name_admin>;

SHOW DATABASES;

-----------------------------------------------------------------------------------------------------------------------

-- USERADMIN Role

-- Create user
CREATE USER <user_name> PASSWORD = '<password>'
DEFAULT_ROLE = ACCOUNTADMIN
MUST_CHANGE_PASSWORD = TRUE;

-- Grant Role to User
GRANT ROLE <role_name_admin> TO USER <user_name>;
-- Grant does not execute due to insufficient privileges

-- To view ownership of roles
SHOW ROLES;

-----------------------------------------------------------------------------------------------------------------------

-- Note: If we have multiple roles assigned to our user we can always change the role using the interface or by SQL
--       command "USE ROLE ..."
-- Note: If a user has the role ACCOUNTADMIN assigned it is not a best practice to always use this role