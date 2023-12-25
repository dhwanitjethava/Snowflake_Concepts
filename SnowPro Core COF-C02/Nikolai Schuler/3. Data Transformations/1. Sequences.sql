USE MANAGE_DB.PUBLIC;

-- Create Sequence
CREATE OR REPLACE SEQUENCE my_seq
    START = 1
    INCREMENT = 1;

SELECT my_seq.nextval;
SELECT my_seq.nextval,my_seq.nextval;

CREATE OR REPLACE TABLE sequence_test (
    id INT DEFAULT my_seq.nextval, 
    first_name VARCHAR
    );

INSERT INTO sequence_test (first_name)
VALUES
    ('Dev'),
    ('Akash');

SELECT * FROM sequence_test;