---- Loading Unstructured Data ----

-- Create stage >> Load raw data (type:VARIANT) >> Analyse & Parse >> Flatten & Load

-- Create stage
CREATE OR REPLACE STAGE <external_stage_name>
    URL = '<url>';

-- Create file format
CREATE OR REPLACE FILE FORMAT <file_format_name>
    TYPE = JSON;

-- Create table
CREATE OR REPLACE TABLE <table_name> (
    RAW_FILEVARIANT
    );

-- Load raw data
COPY INTO <table_name>
    FROM @<external_stage_name>
    FILE_FORMAT = <file_format_name>
    FILES = ('file_name.json');

-- Verify raw data y SELECT statement
SELECT * FROM <table_name>;

-- SELECT statement on any field value of JSON data
SELECT RAW_FILE:<attribute_name> FROM <table_name>;  -- where RAW_FILE is the column name of the table <table_name>
SELECT $1:<attribute_name> FROM <table_name>;  -- where $1 is the first column of the table <table_name>

-- SELECT attribute/column - formatted
SELECT RAW_FILE:<attribute_name>::<data_type> AS <alias> FROM <table_name>;
SELECT
    RAW_FILE:<attribute_name>::<data_type> AS <alias>,
    RAW_FILE:<attribute_name>::<data_type> AS <alias>,
    RAW_FILE:<attribute_name>::<data_type> AS <alias>,
    RAW_FILE:<attribute_name>::<data_type> AS <alias>
FROM <table_name>;

-----------------------------------------------------------------------------------------------------------------------

-- Handling nested data (example)
{
    "Skills": [
        "Applied Mathematics",
        "MTBF",
        "MICROS"
        ],
    "age": 27,
    "department": "Research and Development",
    "first_name": "Tori",
    "id": 19,
    "last_name": "Domel"
};

SELECT 
    RAW_FILE:id::NUMBER AS id, 
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:last_name::STRING AS last_name,
    RAW_FILE:age::NUMBER AS age,
    RAW_FILE:department::STRING AS department,
    TRIM(
    CONCAT(
        COALESCE(RAW_FILE:Skills[0]::STRING,''),', ',
        COALESCE(RAW_FILE:Skills[1]::STRING,''),', ',
        COALESCE(RAW_FILE:Skills[2]::STRING,'')
        ), ', ') AS SKILL
FROM EXERCISE_DB.PUBLIC.JSON_RAW;

-- TRIM function -- remove characters, including white space from string
SELECT LTRIM(RTRIM('##Dhwanit$$', '$$'), '##') AS NAME;  -- Output: Dhwanit

-- REGEXP_REPLACE function
SELECT REGEXP_REPLACE('###DHWA$0$NIT$$','[^a-zA-Z]+','') AS NAME;

-----------------------------------------------------------------------------------------------------------------------

-- Dealing with hierarchy
[
    {
      "language":"English",
      "level":"Fluent"
    },
    {
      "language":"Hindi",
      "level":"Advanced"
    },
    {
      "language":"Gujarati",
      "level":"Advanced"
    }
];

SELECT
    RAW_FILE:id::NUMBER AS id,
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:spoken_languages[0].language::STRING AS first_language,
    RAW_FILE:spoken_languages[0].level::STRING AS level_spoken
FROM EXERCISE_DB.PUBLIC.JSON_RAW
UNION ALL
SELECT
    RAW_FILE:id::NUMBER AS id,
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:spoken_languages[1].language::STRING AS first_language,
    RAW_FILE:spoken_languages[1].level::STRING AS level_spoken
FROM EXERCISE_DB.PUBLIC.JSON_RAW
UNION ALL
SELECT
    RAW_FILE:id::NUMBER AS id,
    RAW_FILE:first_name::STRING AS first_name,
    RAW_FILE:spoken_languages[2].language::STRING AS first_language,
    RAW_FILE:spoken_languages[2].level::STRING AS level_spoken
FROM EXERCISE_DB.PUBLIC.JSON_RAW
ORDER BY ID;

-- FLATTEN function
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.value:language::STRING AS First_language,
    f.value:level::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(FLATTEN(RAW_FILE:spoken_languages)) f;

-----------------------------------------------------------------------------------------------------------------------

-- Copy JSON data into table

-- CREATE TABLE statement
CREATE OR REPLACE TABLE <table_name> AS
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.value:language::STRING AS First_language,
    f.value:level::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(FLATTEN(RAW_FILE:spoken_languages)) f;

-- INSERT INTO statement
INSERT INTO <table_name>
SELECT
    RAW_FILE:first_name::STRING AS First_name,
    f.value:language::STRING AS First_language,
    f.value:level::STRING AS Level_spoken
FROM OUR_FIRST_DB.PUBLIC.JSON_RAW, TABLE(FLATTEN(RAW_FILE:spoken_languages)) f;

-----------------------------------------------------------------------------------------------------------------------

-- Querying PARQUET data

-- Create file format and stage object
CREATE OR REPLACE FILE FORMAT <PARQUET_FORMAT>
    TYPE = PARQUET;

CREATE OR REPLACE STAGE <external_stage_name>
    URL = 'url'   
    FILE_FORMAT = <PARQUET_FORMAT>;

-- Preview the data
LIST @<external_stage_name>;
SELECT * FROM @<external_stage_name>;

-- PARQUET file
{
  "__index_level_0__": 7,
  "cat_id": "HOBBIES",
  "d": 489,
  "date": 1338422400000000,
  "dept_id": "HOBBIES_1",
  "id": "HOBBIES_1_008_CA_1_evaluation",
  "item_id": "HOBBIES_1_008",
  "state_id": "CA",
  "store_id": "CA_1",
  "value": 12
};

-- Syntax for Querying Unstructured data
SELECT 
    $1:__index_level_0__,
    $1:cat_id,
    $1:d,
    $1:"date",
    $1:"dept_id",
    $1:"id",
    $1:"item_id",
    $1:"state_id",
    $1:"store_id",
    $1:"value"
FROM @<external_stage_name>;

-- Querying with conversions and aliases
SELECT
    $1:__index_level_0__::NUMBER AS index_level,
    $1:cat_id::VARCHAR(50) AS cat_id,
    DATE($1:date::NUMBER) AS date,
    $1:dept_id::VARCHAR(50) AS dept_id,
    $1:id::VARCHAR(50) AS id,
    $1:item_id::VARCHAR(50) AS item_id,
    $1:state_id::VARCHAR(50) AS state_id,
    $1:store_id::VARCHAR(50) AS store_id,
    $1:value::int AS value
FROM @<external_stage_name>;

-- Adding Metadata
SELECT
    $1:__index_level_0__::NUMBER AS index_level,
    $1:cat_id::VARCHAR(50) AS cat_id,
    DATE($1:date::NUMBER) AS date,
    $1:dept_id::VARCHAR(50) AS dept_id,
    $1:id::VARCHAR(50) AS id,
    $1:item_id::VARCHAR(50) AS item_id,
    $1:state_id::VARCHAR(50) AS state_id,
    $1:store_id::VARCHAR(50) AS store_id,
    $1:value::int AS value,
    METADATA$FILENAME AS FILENAME,
    METADATA$FILE_ROW_NUMBER AS ROWNUMBER,
    TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP) AS LOAD_DATE
FROM @<external_stage_name>;

-- Create destination table
CREATE OR REPLACE TABLE <destination_table> (
    ROW_NUMBER NUMBER,
    index_level NUMBER,
    cat_id VARCHAR(50),
    date DATE,
    dept_id VARCHAR(50),
    id VARCHAR(50),
    item_id VARCHAR(50),
    state_id VARCHAR(50),
    store_id VARCHAR(50),
    value INT,
    load_date TIMESTAMP DEFAULT TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    );

-- COPY INTO destination table
COPY INTO <destination_table> FROM (
    SELECT
        METADATA$FILE_ROW_NUMBER,
        $1:__index_level_0__::NUMBER,
        $1:cat_id::VARCHAR(50),
        DATE($1:date::NUMBER),
        $1:dept_id::VARCHAR(50),
        $1:id::VARCHAR(50),
        $1:item_id::VARCHAR(50),
        $1:state_id::VARCHAR(50),
        $1:store_id::VARCHAR(50),
        $1:value::int,
        TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    FROM @<external_stage_name>
    );

-----------------------------------------------------------------------------------------------------------------------