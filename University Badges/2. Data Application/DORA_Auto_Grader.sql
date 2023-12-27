select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW01' as step
 ,( select count(*) 
   from PC_RIVERY_DB.INFORMATION_SCHEMA.SCHEMATA 
   where schema_name ='PUBLIC') as actual
 , 1 as expected
 ,'Rivery is set up' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW02' as step
 ,( select count(*) 
   from PC_RIVERY_DB.INFORMATION_SCHEMA.TABLES 
   where ((table_name ilike '%FORM%') 
   and (table_name ilike '%RESULT%'))) as actual
 , 1 as expected
 ,'Rivery form results table is set up' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW03' as step
 ,( select sum(round(nutritions_sugar)) 
   from PC_RIVERY_DB.PUBLIC.FRUITYVICE) as actual
 , 35 as expected
 ,'Fruityvice table is perfectly loaded' as description
);

-- Below GRADER is only run in local worksheet
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW04' as step
 ,( select demo_db.public.sum_mystery_bag_vars($this,$that,$the_other)) as actual
 , 991.5 as expected
 ,'Mystery Bag Function Output' as description
);

-- Below GRADER is only run in local worksheet
select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
 SELECT 'DABW05' as step
 ,( select hash(neutralize_whining('bUt mOm i wAsHeD tHe dIsHes yEsTeRdAy'))) as actual
 , -4759027801154767056 as expected
 ,'WHINGE UDF Works' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
  SELECT 'DABW06' as step
 ,( select count(distinct METADATA$FILENAME) 
   from @demo_db.public.my_internal_named_stage) as actual
 , 3 as expected
 ,'I PUT 3 files!' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from (
   SELECT 'DABW07' as step 
   ,( select count(*) 
     from pc_rivery_db.public.fruit_load_list 
     where fruit_name in ('jackfruit','papaya', 'kiwi', 'test', 'from streamlit', 'guava')) as actual 
   , 4 as expected 
   ,'Followed challenge lab directions' as description
); 