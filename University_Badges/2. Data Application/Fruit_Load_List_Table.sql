-- Create a Fruit Load List Table
use role pc_rivery_role;
use warehouse pc_rivery_wh;

create or replace TABLE PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST (
	FRUIT_NAME VARCHAR(25)
	);

-- Add rows to the fruit load list table
insert into PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST (FRUIT_NAME)
values 
	('banana'),
 	('cherry'),
	('strawberry'),
 	('pineapple'),
 	('apple'),
 	('mango'),
 	('coconut'),
 	('plum'),
 	('avocado'),
	('starfruit');

-- Add another rows
insert into PC_RIVERY_DB.PUBLIC.FRUIT_LOAD_LIST (FRUIT_NAME)
values 
	('from streamlit');

select * from fruit_load_list;