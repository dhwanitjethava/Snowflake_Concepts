-- Create a new database and set the context to use the new database
create database LIBRARY_CARD_CATALOG 
comment = 'DWW Lesson 9 ';

use database LIBRARY_CARD_CATALOG;

-- Create Author table
create or REPLACE table AUTHOR (
   AUTHOR_UID NUMBER,
   FIRST_NAME VARCHAR(50),
   MIDDLE_NAME VARCHAR(50),
   LAST_NAME VARCHAR(50)
   );

-- Insert the first two authors into the Author table
insert into AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
	(1, 'Fiona', '','Macdonald'),
	(2, 'Gian','Paulo','Faleschini');

-- Look at your table with it's new rows
select * from AUTHOR;

-- See how the nextval function works
select SEQ_AUTHOR_UID.nextval;

-- Drop and recreate the counter (sequence) so that it starts at 3 
-- then we'll add the other author records to our author table
create or REPLACE sequence "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_AUTHOR_UID" 
start 3 
increment 1 
comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

show sequences;

-- Add the remaining author records and use the nextval function instead 
-- of putting in the numbers
insert into AUTHOR(AUTHOR_UID,FIRST_NAME,MIDDLE_NAME, LAST_NAME) 
Values
	(SEQ_AUTHOR_UID.nextval, 'Laura', 'K','Egendorf'),
	(SEQ_AUTHOR_UID.nextval, 'Jan', '','Grover'),
	(SEQ_AUTHOR_UID.nextval, 'Jennifer', '','Clapp'),
	(SEQ_AUTHOR_UID.nextval, 'Kathleen', '','Petelinsek');


-- Create a new sequence, this one will be a counter for the book table
create or REPLACE sequence "LIBRARY_CARD_CATALOG"."PUBLIC"."SEQ_BOOK_UID" 
start 1 
increment 1 
comment = 'Use this to fill in the BOOK_UID everytime you add a row';

// Create the book table and use the NEXTVAL as the 
// default value each time a row is added to the table
create or replace table BOOK (
	BOOK_UID NUMBER DEFAULT SEQ_BOOK_UID.nextval,
    TITLE VARCHAR(50),
 	YEAR_PUBLISHED NUMBER(4,0)
);

-- Insert records into the book table. You don't have to list anything for the
-- BOOK_UID field because the default setting will take care of it for you
insert into BOOK(TITLE,YEAR_PUBLISHED)
values
	('Food',2001),
	('Food',2006),
	('Food',2008),
	('Food',2016),
	('Food',2015);

-- Create the relationships table
-- this is sometimes called a "Many-to-Many table"
create table BOOK_TO_AUTHOR (
	BOOK_UID NUMBER,
    AUTHOR_UID NUMBER
);

-- Insert rows of the known relationships
insert into BOOK_TO_AUTHOR(BOOK_UID,AUTHOR_UID)
values
	(1,1), -- This row links the 2001 book to Fiona Macdonald
	(1,2), -- This row links the 2001 book to Gian Paulo Faleschini
	(2,3), -- Links 2006 book to Laura K Egendorf
	(3,4), -- Links 2008 book to Jan Grover
	(4,5), -- Links 2016 book to Jennifer Clapp
	(5,6); -- Links 2015 book to Kathleen Petelinsek


-- Check your work by joining the 3 tables together
-- You should get 1 row for every author
select * from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid; 

-- JSON DDL Scripts
use LIBRARY_CARD_CATALOG;

-- Create an Ingestion Table for JSON Data
create table LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON (
	RAW_AUTHOR VARIANT
);

-- Create File Format for JSON Data
create file format LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT 
	TYPE = 'JSON' 
	COMPRESSION = 'AUTO' 
	ENABLE_OCTAL = FALSE
	ALLOW_DUPLICATE = FALSE 
	STRIP_OUTER_ARRAY = TRUE
	STRIP_NULL_VALUES = FALSE 
	IGNORE_UTF8_ERRORS = FALSE;

copy into LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON from @GARDEN_PLANTS.VEGGIES.LIKE_A_WINDOW_INTO_AN_S3_BUCKET/author_with_header.json
	File_Format = ( FORMAT_NAME = 'JSON_FILE_FORMAT' );

select raw_author from author_ingest_json;

-- Returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

-- Returns the data in a way that makes it look like a normalized table
select
	raw_author:AUTHOR_UID,
	raw_author:FIRST_NAME::STRING as FIRST_NAME,
	raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME,
	raw_author:LAST_NAME::STRING as LAST_NAME,
from AUTHOR_INGEST_JSON;

------------------------------------------------------------------------------------------------------

-- Create an Ingestion Table for the NESTED JSON Data
create or replace table LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON (
	"RAW_NESTED_BOOK" VARIANT
	);

copy into LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON from @GARDEN_PLANTS.VEGGIES.LIKE_A_WINDOW_INTO_AN_S3_BUCKET/json_book_author_nested.txt 
	File_Format = (FORMAT_NAME = 'JSON_FILE_FORMAT');

-- Few simple queries
select RAW_NESTED_BOOK from NESTED_INGEST_JSON;

select RAW_NESTED_BOOK:year_published from NESTED_INGEST_JSON;

select RAW_NESTED_BOOK:authors from NESTED_INGEST_JSON;

-- try changing the number in the brackets to return authors from a different row
select RAW_NESTED_BOOK:authors[0].first_name from NESTED_INGEST_JSON;

-- Use these example flatten commands to explore flattening the nested book and author data
select value:first_name from NESTED_INGEST_JSON,
	lateral flatten(input => RAW_NESTED_BOOK:authors);

select value:first_name from NESTED_INGEST_JSON,
	table(flatten(RAW_NESTED_BOOK:authors));

-- Add a CAST command to the fields returned
select value:first_name::varchar, value:last_name::varchar from NESTED_INGEST_JSON,
	lateral flatten(input => RAW_NESTED_BOOK:authors);

-- Assign new column  names to the columns using "as"
select value:first_name::varchar as FIRST_NM, value:last_name::varchar as LAST_NM from NESTED_INGEST_JSON,
	lateral flatten(input => RAW_NESTED_BOOK:authors);

----------------------------------------------------------------------------------------------------------

--Create a new database to hold the Twitter file
create database SOCIAL_MEDIA_FLOODGATES 
comment = 'There\'s so much data from social media - flood warning';

use database SOCIAL_MEDIA_FLOODGATES;

-- Create a table in the new database
create table SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST 
("RAW_STATUS" VARIANT) 
comment = 'Bring in tweets, one row per tweet or status entity';

-- Create a JSON file format in the new database
create file format SOCIAL_MEDIA_FLOODGATES.PUBLIC.JSON_FILE_FORMAT 
	TYPE = 'JSON' 
	COMPRESSION = 'AUTO' 
	ENABLE_OCTAL = FALSE 
	ALLOW_DUPLICATE = FALSE 
	STRIP_OUTER_ARRAY = TRUE 
	STRIP_NULL_VALUES = FALSE 
	IGNORE_UTF8_ERRORS = FALSE;

copy into SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST from @GARDEN_PLANTS.VEGGIES.LIKE_A_WINDOW_INTO_AN_S3_BUCKET/nutrition_tweets.json
	File_Format = (FORMAT_NAME = 'JSON_FILE_FORMAT');

-- select statements
select RAW_STATUS from TWEET_INGEST;

select RAW_STATUS:entities from TWEET_INGEST;

select RAW_STATUS:entities:hashtags from TWEET_INGEST;

-- Explore looking at specific hashtags by adding bracketed numbers
-- This query returns just the first hashtag in each tweet
select RAW_STATUS:entities:hashtags[0].text from TWEET_INGEST;

-- This version adds a WHERE clause to get rid of any tweet that doesn't include any hashtags
select RAW_STATUS:entities:hashtags[0].text from TWEET_INGEST
	where RAW_STATUS:entities:hashtags[0].text is not null;

-- Perform a simple CAST on the created_at key
-- Add an ORDER BY clause to sort by the tweet's creation date
select RAW_STATUS:created_at::DATE from TWEET_INGEST
	order by RAW_STATUS:created_at::DATE;

-- Flatten statements that return the whole hashtag entity
select value from TWEET_INGEST,
	lateral flatten (input => RAW_STATUS:entities:hashtags);

select value from TWEET_INGEST,
	table(FLATTEN(RAW_STATUS:entities:hashtags));

-- Flatten statement that restricts the value to just the TEXT of the hashtag
select value:text from TWEET_INGEST,
	lateral flatten (input => RAW_STATUS:entities:hashtags);
-- Flatten and return just the hashtag text, CAST the text as VARCHAR
select value:text::VARCHAR from TWEET_INGEST,
	lateral flatten (input => RAW_STATUS:entities:hashtags);

-- Flatten and return just the hashtag text, CAST the text as VARCHAR
-- Use the AS command to name the column
select value:text::VARCHAR as THE_HASHTAG from TWEET_INGEST,
	lateral flatten (input => RAW_STATUS:entities:hashtags);

-- Add the Tweet ID and User ID to the returned table
select RAW_STATUS:user:id as USER_ID, RAW_STATUS:id as TWEET_ID, value:text::VARCHAR as HASHTAG_TEXT from TWEET_INGEST
	lateral flatten (input => RAW_STATUS:entities:hashtags);

create or replace view SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED as (
	select RAW_STATUS:user:id as USER_ID,
    RAW_STATUS:id as TWEET_ID,
    value:text::VARCHAR AS HASHTAG_TEXT
from TWEET_INGEST
	lateral flatten (input => RAW_STATUS:entities:hashtags)
);