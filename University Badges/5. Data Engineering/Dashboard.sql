-- Create a CURATED Layer

-- 1. Create a SCHEMA named CURATED in the AGS_GAME_AUDIENCE database
-- 2. Make sure the schema is owned by SYSADMIN

use role SYSADMIN;
create schema AGS_GAME_AUDIENCE.CURATED;

-- Create a New Dashbord named GAME AUDIENCE
-- Then add a Tile

-- Rename the Tile as Gamer Cities
-- Add below query to the tile
select distinct gamer_name, city from ags_game_audience.enhanced.logs_enhanced_uf;

-- Add a Tile named Time of Day Chart
-- Add below query to the tile
select tod_name as time_of_day, count(*) as tally
from ags_game_audience.enhanced.logs_enhanced_uf 
group by  tod_name
order by tally desc;

-- Rolling Up Login and Logout Events with ListAgg
-- The List Agg function can put both login and logout into a single column in a single row
-- If we don't have a logout, just one timestamp will appear
select GAMER_NAME, listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;

-- Windowed Data for Calculating Time in Game Per Player
select GAMER_NAME, game_event_ltz as login,
       lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout,
        coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

-- Code for the Heatmap
--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length >= 10 and game_session_length < 20 then '10 to 19 mins'
            when game_session_length >= 20 and game_session_length < 30 then '20 to 29 mins'
            when game_session_length >= 30 and game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length,
            tod_name
from (
select GAMER_NAME, tod_name, game_event_ltz as login,
       lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout,
        coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;
