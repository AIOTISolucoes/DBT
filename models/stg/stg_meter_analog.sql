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
    from {{ source('public','raw_meter') }} r
    {% if is_incremental() %}
      where r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
)

select
    timestamp,
    power_plant_id,
    device_id,

    -- Potências
    (json_data ->> 'active_power')::numeric           as active_power_kw,
    (json_data ->> 'power_reactive')::numeric         as power_reactive_kvar,
    (json_data ->> 'apparent_power')::numeric         as apparent_power_kva,
    (json_data ->> 'power_factor')::numeric           as power_factor,

    -- Frequência
    (json_data ->> 'frequency')::numeric              as frequency_hz,

    -- Energias
    (json_data ->> 'exported_active_energy')::numeric    as exported_active_energy_kwh,
    (json_data ->> 'imported_active_energy')::numeric    as imported_active_energy_kwh,
    (json_data ->> 'exported_reactive_energy')::numeric  as exported_reactive_energy_kvarh,
    (json_data ->> 'imported_reactive_energy')::numeric  as imported_reactive_energy_kvarh,

    -- Correntes
    (json_data ->> 'current_phase_a')::numeric        as current_phase_a_a,
    (json_data ->> 'current_phase_b')::numeric        as current_phase_b_a,
    (json_data ->> 'current_phase_c')::numeric        as current_phase_c_a,

    -- Tensões
    (json_data ->> 'line_voltage_ab')::numeric        as line_voltage_ab_v,
    (json_data ->> 'line_voltage_bc')::numeric        as line_voltage_bc_v,
    (json_data ->> 'line_voltage_ca')::numeric        as line_voltage_ca_v

from base
