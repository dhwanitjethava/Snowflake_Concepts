DESC STAGE MANAGE_DB.external_stages.aws_stage;

-- Creating Schema to keep things organized
CREATE OR REPLACE SCHEMA MANAGE_DB.file_formats;

-- Creating file-format object
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.my_file_format
    TYPE = CSV;

-- See properties of file-format object
DESC FILE FORMAT MANAGE_DB.file_formats.my_file_format;

-- Create stage with file-format object
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
    URL= 's3://bucketsnowflakes3'
    FILE_FORMAT = (FORMAT_NAME = MANAGE_DB.file_formats.my_file_format);

-- Reset table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );    

-- Specifying file-format in Copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    files = ('OrderDetails.csv');

-- Altering file format object
ALTER file format MANAGE_DB.file_formats.my_file_format
    SET SKIP_HEADER = 1;

-- Now run Copy command
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.external_stages.aws_stage
    files = ('OrderDetails.csv');
