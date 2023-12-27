create or replace api integration dora_api_integration
	api_provider = aws_api_gateway
	api_aws_role_arn = 'arn:aws:iam::32xxxxxxxx30:role/snowflakeLearnerAssumedRole'
	enabled = true
	api_allowed_prefixes = ('https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora');

show integrations;

-- Grader Function
create or replace external function demo_db.public.grader (
	step varchar,
    	passed boolean,
    	actual integer,
    	expected integer,
    	description varchar
	)
returns variant
api_integration = dora_api_integration
context_headers = (current_timestamp, current_account, current_statement) as 'https://awy6hshxy4.execute-api.us-west-2.amazonaws.com/dev/edu_dora/grader';

-- Check DORA is working or not!
select grader(step, (actual = expected), actual, expected, description) as graded_results from
	(select 
	 'DORA_IS_WORKING' as step,
	 (select 123) as actual,
	 123 as expected,
	 'Dora is working!' as description
	);

select * from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA;

select * from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');

select count(*) as SCHEMAS_FOUND, '3' as SCHEMAS_EXPECTED from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');
