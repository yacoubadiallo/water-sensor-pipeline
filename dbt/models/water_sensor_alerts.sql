with base as (

    select *
    from {{ source('public', 'water_sensor_agg') }}

)

select
    start_time,
    pipeline_id,
    location,
    avg_pressure,
    avg_flow_rate,
    avg_water_quality,

    case
        when avg_pressure > 4 then 'HIGH_PRESSURE'
        when avg_flow_rate < 10 then 'LOW_FLOW'
        when avg_water_quality < 0.7 then 'WATER_CONTAMINATION'
        else 'NORMAL'
    end as alert_type

from base