with data as (

select
    start_time,
    pipeline_id,
    location,
    avg_pressure,
    avg_flow_rate,
    avg_water_quality
from {{ ref('water_sensor_agg') }}

)

select
    count(*) as total_records,
    avg(avg_pressure) as avg_pressure,
    avg(avg_flow_rate) as avg_flow_rate,
    avg(avg_water_quality) as avg_quality
from data