-- Step 2: Parse & Analyse Raw JSON 

-- Selecting attribute/column
SELECT RAW_FILE:city FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
SELECT $1:first_name FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Selecting attribute/column - formattted
SELECT RAW_FILE:first_name::string AS first_name FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
SELECT RAW_FILE:id::int AS id FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Put everything together
SELECT 
    RAW_FILE:id::INT AS id,
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:last_name::STRING AS last_name,
    RAW_FILE:gender::STRING AS gender
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

--Handling nested data
SELECT RAW_FILE:job AS job FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
SELECT 
      RAW_FILE:job.salary::INT AS salary
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:job.salary::INT AS salary,
    RAW_FILE:job.title::STRING AS title
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

-- Handling arrays
SELECT
    RAW_FILE:prev_company AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;
SELECT
    RAW_FILE:prev_company[1]::STRING AS prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::INT AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[0]::STRING AS first_prev_company,
    RAW_FILE:prev_company[1]::STRING AS second_prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW;

SELECT 
    RAW_FILE:id::INT AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[0]::STRING AS first_prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
UNION ALL 
SELECT 
    RAW_FILE:id::INT AS id,  
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:prev_company[1]::STRING AS second_prev_company
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW
ORDER BY id;