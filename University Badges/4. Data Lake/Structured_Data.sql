-- Create a Database for Zena's Athleisure Idea
create or replace database ZENAS_ATHLEISURE_DB;

-- Drop PUBLIC schema
drop schema ZENAS_ATHLEISURE_DB.PUBLIC;

-- Create a new Schema PRODUCTS
create schema PRODUCTS;

-- Create a Stage to Access the Sweat Suit Images
create stage UNI_KLAUS_CLOTHING
    url = 's3://uni-klaus/clothing';

-- Show all files in stage
list @UNI_KLAUS_CLOTHING;

-- Create another Stage for another of Klaus' folders!
create stage UNI_KLAUS_ZMD
    url = 's3://uni-klaus/zenas_metadata';

-- Show all files in stage
list @UNI_KLAUS_ZMD;

-- Create A 3rd Stage!
create stage UNI_KLAUS_SNEAKERS
    url = 's3://uni-klaus/sneakers';

-- Show all files in stage
list @UNI_KLAUS_SNEAKERS;
list @UNI_KLAUS_ZMD;

-- Let's just see what appears in the first column ($1) of each file
select $1 from @UNI_KLAUS_ZMD;

-- Query Data in Just One File at a Time
select $1 from @uni_klaus_zmd/product_coordination_suggestions.txt;
select $1 from @uni_klaus_zmd/sweatsuit_sizes.txt;
select $1 from @uni_klaus_zmd/swt_product_line.txt;

-- Create an Exploratory File Format
-- Let's create a file format to test whether the carets are supposed to separate one row from another.
create file format zmd_file_format_1
RECORD_DELIMITER = '^';

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_1);

-- What if the carets aren't the row separators? What if they are the column separators, instead?
create file format zmd_file_format_2
FIELD_DELIMITER = '^';  

select $1
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_2);

-- Define both the field delimiter and the row delimiter to make it work.
-- Be sure to replace the question marks with the real delimiters!
create or replace file format zmd_file_format_3
FIELD_DELIMITER = '='
RECORD_DELIMITER = '^'; 

select $1, $2
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

-- Rewrite zmd_file_format_1 to parse sweatsuit_sizes.txt
create or replace file format zmd_file_format_1
RECORD_DELIMITER = ';'
TRIM_SPACE = true;;

select $1 as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1);

-- Rewrite zmd_file_format_2 to parse swt_product_line.txt
create or replace file format zmd_file_format_2
FIELD_DELIMITER = '|'
RECORD_DELIMITER = ';'
TRIM_SPACE = true;

select $1, $2, $3
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

-- Dealing with Unexpected Characters
-- In SQL we can use ASCII references to deal with these characters
-- -- 13 is the ASCII for Carriage return
-- -- 10 is the ASCII for Line Feed

-- SQL has a function, CHR() that will allow you to reference ASCII characters by their numbers.
-- So, chr(13) is the same as the Carriage Return character and chr(10) is the same as the Line Feed character. 

-- In Snowflake, we can CONCATENATE two values by putting || between them (a double pipe).

select replace ($1, chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1)
where sizes_available <> '';

select replace ($1, chr(13)||chr(10)), $2, $3
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

-- Convert Your Select to a View
create or replace view zenas_athleisure_db.products.sweatsuit_sizes as
select replace ($1, chr(13)||chr(10)) as sizes_available
from @uni_klaus_zmd/sweatsuit_sizes.txt
(file_format => zmd_file_format_1)
where sizes_available <> '';

select * from zenas_athleisure_db.products.sweatsuit_sizes;

-- Challange : Make the Sweatband Product Line File Look Great!
create or replace view zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE as
select replace ($1, chr(13)||chr(10)) as product_code, $2 as headband_description, $3 as wristband_description
from @uni_klaus_zmd/swt_product_line.txt
(file_format => zmd_file_format_2);

select * from zenas_athleisure_db.products.SWEATBAND_PRODUCT_LINE;

-- Make the Product Coordination Data Look great!
create or replace view zenas_athleisure_db.products.SWEATBAND_COORDINATION as
select $1 as product_code, $2 as has_matching_sweatsuit
from @uni_klaus_zmd/product_coordination_suggestions.txt
(file_format => zmd_file_format_3);

-- SELECT on view SWEATBAND_COORDINATION
select * from zenas_athleisure_db.products.SWEATBAND_COORDINATION;