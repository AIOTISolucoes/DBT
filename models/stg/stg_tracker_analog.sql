{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id']
) }}

with base as (
    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        r.json_data
    from {{ source('public','raw_tracker') }} r
    {% if is_incremental() %}
      where r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
)

select
    timestamp,
    power_plant_id,
    device_id,

    -- Identificação / vínculo (dados técnicos do campo)
    (json_data ->> 'inverter_id')::int     as inverter_id,
    (json_data ->> 'serial_number')        as serial_number,
    (json_data ->> 'manufacturer')         as manufacturer,

    -- Campos analógicos
    (json_data ->> 'angular_position_current')::numeric as angular_position_current_deg,
    (json_data ->> 'angular_position_target')::numeric  as angular_position_target_deg

from base
