-- Use PARSE_JSON to interprete a string as JSON document
SELECT PARSE_JSON('{"key1":"value1", "key2":"value2" }');

CREATE OR REPLACE TABLE semi_structured(
    data VARIANT
    );

-- Insert data using PARSE_JSON
INSERT INTO semi_structured SELECT PARSE_JSON(' { "key1": "value1", "key2": "value2" } ');

-- Query from table
SELECT data:key1 FROM semi_structured;

-- Query data
SELECT * FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Insert more data into table
INSERT INTO OUR_FIRST_DB.PUBLIC.JSON_RAW SELECT (PARSE_JSON(' 
 {   "id":9,
     "first_name":"Kelcey",
     "last_name":"Pavlenko",
     "gender":"Male",
     "city":"Zhuyeping",
     "job":{"title":"Pharmacist","salary":31100},
     "spoken_languages":["Nepali","English"]},
 '));

SELECT 
    RAW_FILE:id::INT AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    VALUE::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW,
TABLE(FLATTEN(input => RAW_FILE:prev_company ));

-- lateral flatten
SELECT 
    RAW_FILE:id::INT AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    VALUE::STRING as prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW,
LATERAL FLATTEN(input => RAW_FILE:prev_company );