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
    from {{ ref('int_tracker_analog') }}
)

select
    power_plant_id,
    power_plant_name,

    device_id   as tracker_id,
    device_name as tracker_name,

    inverter_id,
    inverter_name,

    serial_number,
    manufacturer,

    timestamp as last_update,

    angular_position_current_deg,
    angular_position_target_deg,

    angular_position_current_deg
      - angular_position_target_deg
        as position_error_deg

from latest
where rn = 1
