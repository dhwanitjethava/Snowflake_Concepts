-- Create & Set a Local SQL Variable
set mystery_bag = 'What is in here?';

-- Run a Select that Displays the Variable
select $mystery_bag;

-- Change the Value and Run the Select Again
set mystery_bag = 'This bag is empty!!';
select $mystery_bag;

-- Do more with more variables
set var1 = 2;
set var2 = 5;
set var3 = 7;

select $var1 + $var2 + $var3 as total;

-------------------------------------------------------------------------------------------------

-- Create a Simple User Defined Function (UDF)
create function sum_mystery_bag_vars (var1 number, var2 number, var3 number)
	returns number as 'select var1+var2+var3';

-- Run Function
select sum_mystery_bag_vars(12,35,204);

-- Combine Local Variables & Function Calls
set one = 21;
set two = 34;
set three = -20;

select sum_mystery_bag_vars($one,$two,$three);

-------------------------------------------------------------------------------------------------

-- DORA Code Check
-- Set these local variables according to the instructions
set this = -10.5;
set that = 2;
set the_other = 1000;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW04' as step
 ,( select demo_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);

-------------------------------------------------------------------------------------------------

-- Using a System Function to Fix a Variable Value
set alternating_caps_phrase = 'aLtErNaTiNg CaPs!';
select $alternating_caps_phrase;

set alternating_caps_phrase = 'wHy ArE yOu lIkE tHis?';
select initcap($alternating_caps_phrase);

-------------------------------------------------------------------------------------------------

-- Challange Lab : Write a UDF that Neutralizes Alternating Caps Phrases!
create or replace function DEMO_DB.PUBLIC.NEUTRALIZE_WHINING(input_text TEXT)
	returns TEXT as 'select initcap(input_text)';

select NEUTRALIZE_WHINING('DHWANIT');

-------------------------------------------------------------------------------------------------

-- DORA Code Check
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW05' as step
 ,( select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))) as actual
 , -4759027801154767056 as expected
 ,'WHINGE UDF Works' as description
);