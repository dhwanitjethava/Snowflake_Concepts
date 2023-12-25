-- Stream example: INSERT
CREATE OR REPLACE DATABASE STREAMS_DB;

-- Create example table 1
CREATE OR REPLACE TABLE sales_staging (
    id VARCHAR,
    product VARCHAR,
    price VARCHAR,
    amount VARCHAR,
    store_id VARCHAR
    );
  
-- Insert values 
INSERT INTO sales_staging 
VALUES
    (1,'Banana',1.99,1,1),
    (2,'Lemon',0.99,1,1),
    (3,'Apple',1.79,1,2),
    (4,'Orange Juice',1.89,1,2),
    (5,'Cereals',5.98,2,1);  

-- Create second table
CREATE OR REPLACE TABLE STORE_TABLE (
    store_id NUMBER,
    location VARCHAR,
    employees NUMBER
    );

INSERT INTO STORE_TABLE
VALUES
    (1,'Chicago',33),
    (2,'London',12);

-- Create final input table
CREATE OR REPLACE TABLE sales_final (
    id INT,
    product VARCHAR,
    price NUMBER,
    amount INT,
    store_id INT,
    location VARCHAR,
    employees INT
    );

 -- Insert into final table
INSERT INTO sales_final
SELECT 
    SA.id,
    SA.product,
    SA.price,
    SA.amount,
    ST.STORE_ID,
    ST.LOCATION, 
    ST.EMPLOYEES
FROM SALES_STAGING SA
JOIN STORE_TABLE ST
ON ST.STORE_ID=SA.STORE_ID;

SELECT * FROM sales_final;

-- Create a stream object
CREATE OR REPLACE STREAM sales_stream ON TABLE sales_staging;

SHOW STREAMS;
DESC STREAM sales_stream;

-- Get changes on data using stream
SELECT * FROM sales_stream;
-- Query table
SELECT * FROM sales_staging;

-- Insert values
INSERT INTO sales_staging  
VALUES
    (6,'Mango',1.99,1,2),
    (7,'Garlic',0.99,1,1);

-- Consume stream object
INSERT INTO sales_final 
SELECT 
    SA.id,
    SA.product,
    SA.price,
    SA.amount,
    ST.STORE_ID,
    ST.LOCATION, 
    ST.EMPLOYEES
FROM SALES_STREAM SA
JOIN STORE_TABLE ST
ON ST.STORE_ID = SA.STORE_ID ;

-- Get changes on data using stream (INSERTS)
SELECT * FROM sales_stream;

---- Combine Streams and Tasks ----

CREATE OR REPLACE TASK sales_stream_task
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('SALES_STREAM')
    AS 
    INSERT INTO sales_final 
        SELECT 
            SA.id,
            SA.product,
            SA.price,
            SA.amount,
            ST.STORE_ID,
            ST.LOCATION, 
            ST.EMPLOYEES 
        FROM SALES_STREAM SA
        JOIN STORE_TABLE ST
        ON ST.STORE_ID = SA.STORE_ID;

ALTER TASK sales_stream_task RESUME;
SHOW TASKS;

-- Change data

INSERT INTO SALES_STAGING VALUES (11,'Milk',1.99,1,2);
INSERT INTO SALES_STAGING VALUES (12,'Chocolate',4.49,1,2);
INSERT INTO SALES_STAGING VALUES (13,'Cheese',3.89,1,1);

-- Verify results
SELECT * FROM SALES_STAGING; 
SELECT * FROM SALES_STREAM;
SELECT * FROM SALES_FINAL;