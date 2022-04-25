{{ config(materialized='table') }}

WITH user_leaderboard_source_data AS (
    SELECT 
        u.userid, 
        count(s.videoid) AS total_submissions,
        sum(s."views") AS total_skips,justify_interval(SUM((((endtime - starttime) * views) || 'second')::INTERVAL)::INTERVAL) AS time_saved 
    FROM {{ source('sbdb','sponsortimes') }} s 
    JOIN {{ source('sbdb','usernames') }} u 
        ON u.userid =s.userid 
    GROUP BY u.userid 

)

SELECT 
    u.username,
    ul.total_submissions,
    ul.total_skips,
    ul.time_saved
FROM user_leaderboard_source_data ul
JOIN {{ source('sbdb','usernames') }} u 
    ON u.userid=ul.userid
ORDER BY ul.time_saved DESC