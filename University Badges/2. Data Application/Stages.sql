-- CMD Command to connect SnowSQL
C:\Users\BAPS>snowsql -a zn44018.ap-southeast-2 -u dhwanitjethava
Password :
* SnowSQL * v1.2.24
Type SQL statements or !help
dhwanitjethava#COMPUTE_WH@(no database).(no schema)>show STAGES;
dhwanitjethava#COMPUTE_WH@(no database).(no schema)>put file://one.txt @demo_db.PUBLIC.my_internal_named_stage;

-- Show all stages in account
show stages in account;

-- LIST command to view a list of files in your Stage
list @my_internal_named_stage;

-- Perform a SELECT on files that have not even been loaded yet.
select $1 from @my_internal_named_stage/one.txt.gz;
