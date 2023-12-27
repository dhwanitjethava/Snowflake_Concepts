use role accountadmin;

create or replace api integration dora_api_integration
api_provider = aws_api_gateway
api_aws_role_arn = 'arn:aws:iam::321463406630:role/snowflakeLearnerAssumedRole'
enabled = true
api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

create or replace external function demo_db.public.grader(
      step varchar
    , passed boolean
    , actual integer
    , expected integer
    , description varchar)
returns variant
api_integration = dora_api_integration 
context_headers = (current_timestamp,current_account, current_statement) 
as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader';

-- where did you put the function?
show user functions in account;

-- did you put it here?
select * from snowflake.account_usage.functions
where function_name = 'GRADER'
and function_catalog = 'DEMO_DB'
and function_owner = 'ACCOUNTADMIN';

-- set your worksheet drop lists to the location of your GRADER function

--DO NOT EDIT BELOW THIS LINE - Don't add or take away spaces, do change 'from' to 'FROM' - NO EDITS AT ALL
select GRADER(step,(actual = expected), actual, expected, description) as graded_results from (
SELECT 'DORA_IS_WORKING' as step
 ,(select 223 ) as actual
 ,223 as expected
 ,'Dora is working!' as description
); 