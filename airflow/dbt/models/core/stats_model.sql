{{ config(materialized='table') }}
WITH user_count_source_data AS (
    SELECT 
        1 AS uid,'missing data from the source' AS total_active_users,
        COUNT(DISTINCT userid) AS number_of_conributing_users,
        COUNT(*) AS total_submissions
    FROM {{ source('sbdb','sponsortimes') }} 
),
time_saved_source AS (
    SELECT 
        sum(total_time_saved_per_video) as total_time_saved
    FROM (
        SELECT 
            (((endtime - starttime) * views) || 'second')::interval AS total_time_saved_per_video 
        FROM {{ source('sbdb','sponsortimes') }} 
        WHERE actiontype ='skip'
        ) 
    AS total0
),
time_interval AS(
    SELECT 
        1 AS tid,
        justify_interval(total_time_saved::interval) AS total_saved_time 
    FROM time_saved_source
)

SELECT 
    total_active_users,
    number_of_conributing_users,
    total_submissions,
    total_saved_time
FROM user_count_source_data u
FULL OUTER JOIN time_interval t 
    ON u.uid=t.tid