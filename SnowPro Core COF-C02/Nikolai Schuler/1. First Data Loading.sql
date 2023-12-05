-- Create database
CREATE OR REPLACE DATABASE FIRST_DB;

-- Rename database 
ALTER DATABASE FIRST_DB RENAME TO OUR_FIRST_DB;

-- Create the table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT (
    loan_id STRING,
    loan_status STRING,
    principal STRING,
    terms STRING,
    effective_date STRING,
    due_date STRING,
    paid_off_time STRING,
    past_due_days STRING,
    age STRING,
    education STRING,
    gender STRING
    );

 SELECT * FROM OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT;
 
 -- Show tables
 SHOW TABLES;
 
 -- Loading the data from S3 bucket
 COPY INTO OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT
    FROM s3://bucketsnowflakes3/Loan_payments_data.csv
    file_format = (type = csv 
                   field_delimiter = ',' 
                   skip_header = 1
                   );

-- Validate the data
 SELECT * FROM OUR_FIRST_DB.PUBLIC.LOAN_PAYMENT;
