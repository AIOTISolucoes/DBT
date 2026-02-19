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
    from {{ source('public','raw_nobreak') }} r
    {% if is_incremental() %}
      where r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
)

select
    timestamp,
    power_plant_id,
    device_id,

    -- Entrada
    (json_data ->> 'input_voltage')::numeric         as input_voltage_v,
    (json_data ->> 'input_frequency')::numeric       as input_frequency_hz,

    -- Saída
    (json_data ->> 'output_voltage')::numeric        as output_voltage_v,
    (json_data ->> 'output_frequency')::numeric      as output_frequency_hz,
    (json_data ->> 'output_current')::numeric        as output_current_a,
    (json_data ->> 'output_power')::numeric          as output_power_kw,
    (json_data ->> 'output_apparent_power')::numeric as output_apparent_power_kva,

    -- Bateria
    (json_data ->> 'battery_voltage')::numeric       as battery_voltage_v,
    (json_data ->> 'battery_capacity')::numeric      as battery_capacity_pct,
    (json_data ->> 'battery_temperature')::numeric   as battery_temperature_c,

    -- Carga / autonomia
    (json_data ->> 'load_percentage')::numeric        as load_percentage_pct,
    (json_data ->> 'remaining_backup_time')::numeric  as remaining_backup_time_min

from base
