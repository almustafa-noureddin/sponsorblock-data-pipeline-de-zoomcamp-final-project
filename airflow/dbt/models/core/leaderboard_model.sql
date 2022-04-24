{{ config(materialized='table') }}

WITH user_leaderboard_source_data as (
select u.userid, count(s.videoid) as total_submissions,sum(s."views")as total_skips,justify_interval(sum((((endtime - starttime) * views) || 'second')::interval)::interval) as time_saved 
from {{ source('sbdb','sponsortimes') }} s 
join {{ source('sbdb','usernames') }} u on u.userid =s.userid 
group by u.userid 

)

select u.username,ul.total_submissions,ul.total_skips,ul.time_saved
from user_leaderboard_source_data ul
join {{ source('sbdb','usernames') }} u on u.userid=ul.userid
order by ul.time_saved desc