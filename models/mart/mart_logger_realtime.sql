{{ config(
    materialized = 'view'
) }}

with latest as (

    select
        *,
        row_number() over (
            partition by power_plant_id
            order by timestamp desc
        ) as rn
    from {{ ref('int_logger_analog') }}
)

select
    power_plant_id,
    power_plant_name,

    device_id,
    device_name,
    device_type,

    timestamp as last_update,

    -- Potências
    active_power_kw,
    power_reactive_kvar,
    apparent_power_kva,
    power_factor,

    -- Energias (contadores brutos)
    exported_active_energy_kwh,
    imported_active_energy_kwh,

    -- Correntes
    current_phase_a_a,
    current_phase_b_a,
    current_phase_c_a,

    -- Tensões
    line_voltage_ab_v,
    line_voltage_bc_v,
    line_voltage_ca_v

from latest
where rn = 1
