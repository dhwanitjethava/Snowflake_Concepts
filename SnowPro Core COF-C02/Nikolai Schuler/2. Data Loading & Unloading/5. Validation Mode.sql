-- Prepare database & table
CREATE OR REPLACE DATABASE COPY_DB;

CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30),
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

-- Prepare stage object
CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    URL = 's3://snowflakebucket-copyoption/size/';

LIST @COPY_DB.PUBLIC.aws_stage_copy;
  
-- Load data using copy command
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER =1)
    PATTERN ='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;

SELECT * FROM ORDERS;    

COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER =1)
    PATTERN ='.*Order.*'
    VALIDATION_MODE = RETURN_5_ROWS;

---- Use files with errors ----

CREATE OR REPLACE STAGE COPY_DB.PUBLIC.aws_stage_copy
    URL ='s3://snowflakebucket-copyoption/returnfailed/';
    
LIST @copy_db.public.aws_stage_copy;

-- Show all errors
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER =1)
    PATTERN ='.*Order.*'
    VALIDATION_MODE = RETURN_ERRORS;

-- Validate first n rows
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER =1)
    PATTERN='.*error.*'
    VALIDATION_MODE = RETURN_1_ROWS;

------------------------------------------------------------------------------------------------------------------------

---- VALIDATE FUNCTION ----

USE DATABASE COPY_DB;

CREATE OR REPLACE TABLE  COPY_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT VARCHAR(30), 
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

LIST @aws_stage_copy;
    
COPY INTO COPY_DB.PUBLIC.ORDERS
    FROM @aws_stage_copy
    FILE_FORMAT = (TYPE = CSV FIELD_DELIMITER = ',' SKIP_HEADER =1)
    ON_ERROR = CONTINUE;

-- Validate function
SELECT * FROM TABLE (VALIDATE(ORDERS, JOB_ID=>'_last'));
