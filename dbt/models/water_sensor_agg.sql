with base as (

    select *
    from {{ source('public', 'water_sensor_agg') }}

)

select
    start_time,
    end_time,
    pipeline_id,
    location,

    avg(avg_pressure) as avg_pressure,
    avg(avg_flow_rate) as avg_flow_rate,
    avg(avg_water_quality) as avg_water_quality,
    avg(avg_temperature) as avg_temperature,

    sum(num_measurements) as num_measurements

from base

group by
    start_time,
    end_time,
    pipeline_id,
    location