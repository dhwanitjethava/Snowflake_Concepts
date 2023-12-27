select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
 SELECT
 'DNGW01' as step
  ,(
      select count(*)  
      from ags_game_audience.raw.logs
      where is_timestamp_ntz(to_variant(datetime_iso8601))= TRUE 
   ) as actual
, 250 as expected
, 'Project DB and Log File Set Up Correctly' as description
);

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
   'DNGW02' as step
   ,( select sum(tally) from(
        select (count(*) * -1) as tally
        from ags_game_audience.raw.logs 
        union all
        select count(*) as tally
        from ags_game_audience.raw.game_logs)     
     ) as actual
   ,250 as expected
   ,'View is filtered' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT
   'DNGW03' as step
   ,( select count(*) 
      from ags_game_audience.enhanced.logs_enhanced
      where dow_name = 'Sat'
      and tod_name = 'Early evening'   
      and gamer_name like '%prajina'
     ) as actual
   ,2 as expected
   ,'Playing the game on a Saturday evening' as description
); 

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW04' as step
 ,( select count(*)/iff (count(*) = 0, 1, count(*))
  from table(ags_game_audience.information_schema.task_history
              (task_name=>'LOAD_LOGS_ENHANCED'))) as actual
 ,1 as expected
 ,'Task exists and has been run at least once' as description 
 );

 select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW05' as step
 ,(
   select max(tally) from (
       select CASE WHEN SCHEDULED_FROM = 'SCHEDULE' 
                         and STATE= 'SUCCEEDED' 
              THEN 1 ELSE 0 END as tally 
   from table(ags_game_audience.information_schema.task_history (task_name=>'GET_NEW_FILES')))
  ) as actual
 ,1 as expected
 ,'Task succeeds from schedule' as description
 );

 select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW06' as step
 ,(
   select CASE WHEN pipe_status:executionState::text = 'RUNNING' THEN 1 ELSE 0 END 
   from(
   select parse_json(SYSTEM$PIPE_STATUS( 'ags_game_audience.raw.GET_NEW_FILES' )) as pipe_status)
  ) as actual
 ,1 as expected
 ,'Pipe exists and is RUNNING' as description
 );

 select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
SELECT
'DNGW07' as step
 ,(
   select 1 
  ) as actual
 ,1 as expected
 ,'We hope you learned about dashboards' as description
 );