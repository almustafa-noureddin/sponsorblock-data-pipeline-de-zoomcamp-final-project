{{ config(materialized='table') }}
WITH user_count_source_data as (
    SELECT 
        1 as uid,'missing data from the source' as total_active_users,
        COUNT(distinct userid) as number_of_conributing_users,
        count(*) as total_submissions
    FROM {{ source('sbdb','sponsortimes') }} 
),
time_saved_source AS (
    select sum(total_time_saved_per_video) as total_time_saved
    from (
        select (((endtime - starttime) * views) || 'second')::interval as total_time_saved_per_video 
        from {{ source('sbdb','sponsortimes') }} 
        where actiontype ='skip'
        ) 
    as total0
),
time_interval as(
    select 1 as tid, justify_interval(total_time_saved::interval) as total_saved_time from time_saved_source
)

select total_active_users,number_of_conributing_users,total_submissions,total_saved_time
FROM user_count_source_data u
full outer join time_interval t on u.uid=t.tid