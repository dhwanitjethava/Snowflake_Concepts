-- Create ORDERS table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.ORDERS (
    ORDER_ID VARCHAR(30),
    AMOUNT INT,
    PROFIT INT,
    QUANTITY INT,
    CATEGORY VARCHAR(30),
    SUBCATEGORY VARCHAR(30)
    );

SELECT * FROM OUR_FIRST_DB.PUBLIC.ORDERS;

-- List files contained in stage
LIST @MANAGE_DB.EXTERNAL_STAGES.aws_stage;

-- COPY command with fully qualified stage object name
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
    FILE_FORMAT = (TYPE = CSV
                   FIELD_DELIMITER = ','
                   SKIP_HEADER = 1)
    FILES = ('OrderDetails.csv');

-- COPY command with pattern for file names
COPY INTO OUR_FIRST_DB.PUBLIC.ORDERS
    FROM @MANAGE_DB.EXTERNAL_STAGES.aws_stage
    FILE_FORMAT = (TYPE = CSV
                   FIELD_DELIMITER = ','
                   SKIP_HEADER = 1)
    PATTERN = '.*Order.*';
