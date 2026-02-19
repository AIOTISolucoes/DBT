{{ config(
    materialized = 'table'
) }}

select
    power_plant_id,
    power_plant_name,

    device_id   as tracker_id,
    device_name as tracker_name,

    inverter_id,
    inverter_name,

    date(timestamp) as date,

    avg(
        abs(
            angular_position_current_deg
          - angular_position_target_deg
        )
    ) as avg_alignment_error_deg,

    max(
        abs(
            angular_position_current_deg
          - angular_position_target_deg
        )
    ) as max_alignment_error_deg

from {{ ref('int_tracker_analog') }}
group by
    power_plant_id,
    power_plant_name,
    tracker_id,
    tracker_name,
    inverter_id,
    inverter_name,
    date
