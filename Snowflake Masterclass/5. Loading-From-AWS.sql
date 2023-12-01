-- Creating policy

-- 1. Create new role for another AWS account 
-- - Account ID : <your_aws_account_id>
-- - Check the both options : Require external ID (write dummy value for now); Require MFA
-- - Attach following permissions policies : AmazonS3FullAccess

-----------------------------------------------------------------------------------------------------------------------

---- Storage Integration Object ----

-- Create integration object
CREATE OR REPLACE STORAGE INTEGRATION <integration_name>
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<role_arn>'
    STORAGE_ALLOWED_LOCATIONS = ('<cloud>://<bucket>/<path>/', '<cloud>://<bucket>/<path>/')
    COMMENT = '<string>';

-- See storage integration properties to fetch external_id so we can update it in S3
DESC INTEGRATION <integration_name>;
-- Snowflake : STORAGE_AWS_IAM_USER_ARN --> AWS>IAM>ROLE>Trust relationships : AWS
-- Snowflake : STORAGE_AWS_EXTERNAL_ID --> AWS>IAM>ROLE>Trust relationships : sts:ExternalId

-----------------------------------------------------------------------------------------------------------------------