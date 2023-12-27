-- Create a database called MELS_SMOOTHIE_CHALLENGE_DB
create database MELS_SMOOTHIE_CHALLENGE_DB;

-- Drop the PUBLIC schema
drop schema MELS_SMOOTHIE_CHALLENGE_DB.PUBLIC;

-- Add a schema named TRAILS
create schema MELS_SMOOTHIE_CHALLENGE_DB.TRAILS;

-- Create Stage TRAILS_GEOJSON
create stage MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.trails_geojson
    url = 's3://uni-lab-files-more/dlkw/trails/trails_geojson';

-- Create Stage TRAILS_PARQUET
create stage MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.trails_parquet
    url = 's3://uni-lab-files-more/dlkw/trails/trails_parquet';

list @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON;
list @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET;

-- Create File Format - FF_JSON
create file format FF_JSON
    type = 'JSON';

-- Create File Format - FF_PARQUET
create file format FF_PARQUET
    type = 'PARQUET';

-- Query Your TRAILS_GEOJSON Stage
select $1 from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON
    (file_format => FF_JSON);
    
-- Query Your TRAILS_PARQUET Stage
select $1 from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
    (file_format => FF_PARQUET);

-- Sophisticated query to parse the data into columns
select 
    $1:sequence_1 as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng, -- we check on this data
    $1:longitude::number(11,8) as lat
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
    (file_format => FF_PARQUET)
order by point_id;

--  Create a View Called CHERRY_CREEK_TRAIL
create view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL as
select 
    $1:sequence_1 as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng, -- we check on this data
    $1:longitude::number(11,8) as lat
from @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET
    (file_format => FF_PARQUET)
order by point_id;

select * from CHERRY_CREEK_TRAIL;

-- Use || to Chain Lat and Lng Together into Coordinate Sets!
-- DOUBLE PIPE can be used to CONCATENATE items into a string.
--Using concatenate to prepare the data for plotting on a map
select top 100
    lng||' '||lat as coord_pair,
    'POINT('||coord_pair||')' as trail_point
from cherry_creek_trail;

-- To add a column, we have to replace the entire view
create or replace view cherry_creek_trail as
select 
     $1:sequence_1 as point_id,
     $1:trail_name::varchar as trail_name,
     $1:latitude::number(11,8) as lng,
     $1:longitude::number(11,8) as lat,
     lng||' '||lat as coord_pair
from @trails_parquet
    (file_format => ff_parquet)
order by point_id;

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL;

-- Let's Collapse Sets Of Coordinates into Linestrings! 
-- Run this SELECT and Paste the Results into WKT Playground!
select 
    'LINESTRING('||
    listagg(coord_pair, ',') 
    within group (order by point_id)
    ||')' as my_linestring
from cherry_creek_trail
where point_id <= 10
group by trail_name;

------------------------------------------------------------------------------------------------------------------------------

-- Look at the geoJSON Data, Normalize the Data Without Loading It!
select
    $1:features[0]:properties:Name::string as feature_name,
    $1:features[0]:geometry:coordinates::string as feature_coordinates,
    $1:features[0]:geometry::string as geometry,
    $1:features[0]:properties::string as feature_properties,
    $1:crs:properties:name::string as specs,
    $1 as whole_object
from @trails_geojson (file_format => ff_json);

-- Create a view : DENVER_AREA_TRAILS
create or replace view DENVER_AREA_TRAILS as 
select
    $1:features[0]:properties:Name::string as feature_name,
    $1:features[0]:geometry:coordinates::string as feature_coordinates,
    $1:features[0]:geometry::string as geometry,
    $1:features[0]:properties::string as feature_properties,
    $1:crs:properties:name::string as specs,
    $1 as whole_object
from @trails_geojson (file_format => ff_json);

select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;

-- Calculate the Lengths for the cherry_creek_trail
select 
    'LINESTRING('||listagg(coord_pair, ',') within group (order by point_id)||')' as my_linestring,
    st_length(TO_GEOMETRY(my_linestring)) as trail_length
from cherry_creek_trail;

-- Calculate the Lengths for the 
select 
    feature_name,
    st_length(TO_GEOGRAPHY(geometry)) as trail_length 
from denver_area_trails;

-- Change your DENVER_AREA_TRAILS view to include a Length Column!
select get_ddl('view', 'DENVER_AREA_TRAILS');

create or replace view DENVER_AREA_TRAILS(
	FEATURE_NAME,
	FEATURE_COORDINATES,
	GEOMETRY,
    TRAIL_LENGTH,
	FEATURE_PROPERTIES,
	SPECS,
	WHOLE_OBJECT
) as 
select
    $1:features[0]:properties:Name::string as feature_name,
    $1:features[0]:geometry:coordinates::string as feature_coordinates,
    $1:features[0]:geometry::string as geometry,
    st_length(TO_GEOGRAPHY(geometry)) as trail_length,
    $1:features[0]:properties::string as feature_properties,
    $1:crs:properties:name::string as specs,
    $1 as whole_object
from @trails_geojson (file_format => ff_json);

select * from DENVER_AREA_TRAILS;

------------------------------------------------------------------------------------------------------------------------------

-- Create a View on Cherry Creek Data to Mimic the Other Trail Data

-- Create a view that will have similar columns to DENVER_AREA_TRAILS 
-- Even though this data started out as Parquet, and we're joining it with geoJSON data
-- So let's make it look like geoJSON instead.
create view DENVER_AREA_TRAILS_2 as
    select 
        trail_name as feature_name,
        '{"coordinates":['||listagg('['||lng||','||lat||']',',')||'],"type":"LineString"}' as geometry,
        st_length(to_geography(geometry)) as trail_length
    from cherry_creek_trail
    group by trail_name;

-- Use A Union All to Bring the Rows Into a Single Result Set

-- Create a view that will have similar columns to DENVER_AREA_TRAILS 
select feature_name, geometry, trail_length from DENVER_AREA_TRAILS
union all
select feature_name, geometry, trail_length from DENVER_AREA_TRAILS_2;

-- All sessions default to the geograph_output_format='GeoJSON'
-- Alter session output to 'WKT'
alter session set geography_output_format='WKT';

-- Add more GeoSpatial Calculations to get more GeoSpecial Information!
select feature_name,
    to_geography(geometry) as my_linestring,
    st_xmin(my_linestring) as min_eastwest,
    st_xmax(my_linestring) as max_eastwest,
    st_ymin(my_linestring) as min_northsouth,
    st_ymax(my_linestring) as max_northsouth,
    trail_length
from DENVER_AREA_TRAILS
union all
select feature_name,
    to_geography(geometry) as my_linestring,
    st_xmin(my_linestring) as min_eastwest,
    st_xmax(my_linestring) as max_eastwest,
    st_ymin(my_linestring) as min_northsouth,
    st_ymax(my_linestring) as max_northsouth,
    trail_length
from DENVER_AREA_TRAILS_2;

-- Make it a View : TRAILS_AND_BOUNDARIES
create or replace view TRAILS_AND_BOUNDARIES as
select feature_name,
    to_geography(geometry) as my_linestring,
    st_xmin(my_linestring) as min_eastwest,
    st_xmax(my_linestring) as max_eastwest,
    st_ymin(my_linestring) as min_northsouth,
    st_ymax(my_linestring) as max_northsouth,
    trail_length
from DENVER_AREA_TRAILS
union all
select feature_name,
    to_geography(geometry) as my_linestring,
    st_xmin(my_linestring) as min_eastwest,
    st_xmax(my_linestring) as max_eastwest,
    st_ymin(my_linestring) as min_northsouth,
    st_ymax(my_linestring) as max_northsouth,
    trail_length
from DENVER_AREA_TRAILS_2;

select * from TRAILS_AND_BOUNDARIES;

-- A Polygon Can be Used to Create a Bounding Box
select
    min(min_eastwest) as western_edge,
    min(min_northsouth) as southern_edge,
    max(max_eastwest) as eastern_edge,
    max(max_northsouth) as northern_edge
from trails_and_boundaries;

select 'POLYGON(('|| 
    min(min_eastwest)|| ' '||max(max_northsouth)||','||
    max(max_eastwest)|| ' '||max(max_northsouth)||','||
    max(max_eastwest)|| ' '||min(min_northsouth)||','||
    min(min_eastwest)|| ' '||min(min_northsouth)||','||
    min(min_eastwest)|| ' '||max(max_northsouth)||'))' as my_polygon
from trails_and_boundaries;

------------------------------------------------------------------------------------------------------------------------------

-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lat='-104.97300245114094';
set mc_lng='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lat='-105.00840763333615'; 
set loc_lng='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lat,$mc_lng) as melanies_cafe_point;
select st_makepoint($loc_lat,$loc_lng) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(st_makepoint($mc_lat,$mc_lng),st_makepoint($loc_lat,$loc_lng)) as mc_to_cp;

------------------------------------------------------------------------------------------------------------------------------

-- Let's Create a UDF for Measuring Distance from Melanie's Café

-- Create Schema in Mel's Database and call it LOCATIONS.
-- Make sure it is owned by SYSADMIN.
create schema LOCATIONS;

-- Create UDF : DISTANCE_TO_MC (for Distance to Melanie's Café)
create or replace function DISTANCE_TO_MC(loc_lat number(38,32), loc_lng number(38,32))
  returns float
  as
  $$
  st_distance(st_makepoint('-104.97300245114094','39.76471253574085'),st_makepoint(loc_lat,loc_lng))
  $$;

-- Test the New Function!
-- Tivoli Center into the variables 
set tc_lat='-105.00532059763648'; 
set tc_lng='39.74548137398218';

select distance_to_mc($tc_lat,$tc_lng);

------------------------------------------------------------------------------------------------------------------------------

-- Create a List of Competing Juice Bars in the Area
select * from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

-- Convert the List into a View
create or replace view COMPETITION as
select * from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');

select * from MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.COMPETITION;

-- Which Competitor is Closest to Melanie's?
select name, cuisine, ST_DISTANCE(st_makepoint('-104.97300245114094','39.76471253574085'),
    coordinates) as distance_from_melanies, *
from  competition
order by distance_from_melanies;

-- Changing the Function to Accept a GEOGRAPHY Argument 
create or replace function distance_to_mc(lat_and_lng GEOGRAPHY)
  returns float
  as
  $$
   st_distance(st_makepoint('-104.97300245114094','39.76471253574085'),lat_and_lng)
  $$;
    
-- Now We Can Use it In Our Sonra Select
select name, cuisine, distance_to_mc(coordinates) as distance_from_melanies, *
from  competition
order by distance_from_melanies;

-- Different Options, Same Outcome!

-- Tattered Cover Bookstore McGregor Square
set tcb_lat='-104.9956203'; 
set tcb_lng='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lat,$tcb_lng);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lat,$tcb_lng));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name,distance_to_mc(coordinates) as distance_to_melanies, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';

------------------------------------------------------------------------------------------------------------------------------

-- Challange : Create a View of Bike Shops in the Denver Data
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.DENVER_BIKE_SHOPS as
    select name, ST_DISTANCE(st_makepoint('-104.97300245114094','39.76471253574085'),coordinates) as distance_to_melanies
    from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES
    where shop = 'bicycle'
    order by distance_to_melanies;

------------------------------------------------------------------------------------------------------------------------------

-- Alter the view name CHERRY_CREEK_TRAIL to V_CHERRY_CREEK_TRAIL
alter view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
rename to MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.V_CHERRY_CREEK_TRAIL;

-- Let's Create a Super-Simple, Stripped Down External Table
create or replace external table T_CHERRY_CREEK_TRAIL(
	my_filename varchar(50) as (metadata$filename::varchar(50))
    ) 
location= @trails_parquet
auto_refresh = true
file_format = (type = parquet);

-- Now Let's Modify Our V_CHERRY_CREEK_TRAIL Code to Create the New Table
select get_ddl('view','mels_smoothie_challenge_db.trails.v_cherry_creek_trail');

create or replace view V_CHERRY_CREEK_TRAIL(POINT_ID,TRAIL_NAME,LNG,LAT,COORD_PAIR) as
select 
     $1:sequence_1 as point_id,
     $1:trail_name::varchar as trail_name,
     $1:latitude::number(11,8) as lng,
     $1:longitude::number(11,8) as lat,
     lng||' '||lat as coord_pair
from @trails_parquet
    (file_format => ff_parquet)
order by point_id;

create or replace external table mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL(
	POINT_ID number as ($1:sequence_1::number),
	TRAIL_NAME varchar(50) as  ($1:trail_name::varchar),
	LNG number(11,8) as ($1:latitude::number(11,8)),
	LAT number(11,8) as ($1:longitude::number(11,8)),
	COORD_PAIR varchar(50) as (lng::varchar||' '||lat::varchar)
    ) 
location= @mels_smoothie_challenge_db.trails.trails_parquet
auto_refresh = true
file_format = mels_smoothie_challenge_db.trails.ff_parquet;

--- Create Secure Materialized View SMV_CHERRY_CREEK_TRAIL

create secure materialized view SMV_CHERRY_CREEK_TRAIL
    as select * from mels_smoothie_challenge_db.trails.T_CHERRY_CREEK_TRAIL;
    
-- SELECT on view SMV_CHERRY_CREEK_TRAIL
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;