{{ config(
    materialized = 'view'
) }}

with latest as (

    select
        *,
        row_number() over (
            partition by device_id
            order by timestamp desc
        ) as rn
    from {{ ref('int_nobreak_analog') }}
)

select
    power_plant_id,
    power_plant_name,

    device_id   as nobreak_id,
    device_name as nobreak_name,

    timestamp   as last_update,

    input_voltage_v,
    input_frequency_hz,

    output_voltage_v,
    output_frequency_hz,
    output_current_a,
    output_power_kw,
    output_apparent_power_kva,

    battery_voltage_v,
    battery_capacity_pct,
    battery_temperature_c,

    load_percentage_pct,
    remaining_backup_time_min

from latest
where rn = 1
