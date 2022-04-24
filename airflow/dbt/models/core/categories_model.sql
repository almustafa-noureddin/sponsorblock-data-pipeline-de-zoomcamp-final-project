{{ config(materialized='table') }}

with source_data as (

    select category,count(category) as total
    from {{ source('sbdb','sponsortimes') }}
    group by category
    order by total desc

)

select *
from source_data
