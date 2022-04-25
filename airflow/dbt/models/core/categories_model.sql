{{ config(materialized='table') }}

WITH source_data AS (

    SELECT 
        category,
        count(category) AS total
    FROM {{ source('sbdb','sponsortimes') }}
    GROUP BY category
    ORDER BY total DESC

)

SELECT *
FROM source_data
