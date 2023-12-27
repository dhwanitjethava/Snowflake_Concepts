create or replace stage garden_plants.veggies.like_a_window_into_an_s3_bucket
	url = 's3://uni-lab-files';

list @like_a_window_into_an_s3_bucket;

create or replace table vegetable_details_soil_type (
	plant_name varchar(25),
    	soil_type number(1,0)
    	);

copy into vegetable_details_soil_type from @like_a_window_into_an_s3_bucket
	files = ('VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
	file_format = (format_name = PIPECOLSEP_ONEHEADROW);

--The data in the file, with no FILE FORMAT specified
select $1 from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier
select $1, $2, $3 from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
	(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3 from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
	(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW);

select $1, $2, $3 from @garden_plants.veggies.like_a_window_into_an_s3_bucket/LU_SOIL_TYPE.tsv
	(file_format => garden_plants.veggies.L8_CHALLENGE_FF);

create or replace table LU_SOIL_TYPE (
	SOIL_TYPE_ID number,	
	SOIL_TYPE varchar(15),
	SOIL_DESCRIPTION varchar(75)
    	);

copy into LU_SOIL_TYPE from @like_a_window_into_an_s3_bucket
	files = ('LU_SOIL_TYPE.tsv')
	file_format = (format_name=L8_CHALLENGE_FF);

select * from LU_SOIL_TYPE;

create or replace table vegetable_details_plant_height (
	plant_name varchar,	
	uom varchar(1),
	low_end_of_range number(10),
	high_end_of_range number(10)
	);

copy into vegetable_details_plant_height from @like_a_window_into_an_s3_bucket
	files = ('veg_plant_height.csv')
	file_format = (format_name=COMMASEP_DBLQUOT_ONEHEADROW);

select * from vegetable_details_plant_height;
