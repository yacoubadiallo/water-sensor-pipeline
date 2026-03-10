with base as (

    select *
    from {{ source('public', 'water_sensor_agg') }}

)

select
    location,

    avg(avg_water_quality) as avg_water_quality,
    min(avg_water_quality) as min_water_quality,
    max(avg_water_quality) as max_water_quality,

    case
        when avg(avg_water_quality) < 0.7 then 'BAD'
        when avg(avg_water_quality) < 0.85 then 'MEDIUM'
        else 'GOOD'
    end as quality_status

from base

group by location