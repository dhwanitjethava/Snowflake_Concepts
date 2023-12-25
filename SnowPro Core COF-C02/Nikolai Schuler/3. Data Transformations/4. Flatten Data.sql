-- Flattening data
SELECT * FROM TABLE(FLATTEN(input => [2,4,6])) f;

SELECT 
    RAW_FILE:id::INT AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    VALUE::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW,
TABLE(FLATTEN( input => RAW_FILE:prev_company ));