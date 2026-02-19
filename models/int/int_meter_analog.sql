{{ config(materialized = 'view') }}

select
    a.timestamp,
    a.power_plant_id,
    pp.name            as power_plant_name,

    a.device_id,
    d.name             as device_name,
    dt.name            as device_type,

    -- Potências
    a.active_power_kw,
    a.power_reactive_kvar,
    a.apparent_power_kva,
    a.power_factor,

    -- Frequência
    a.frequency_hz,

    -- Energias
    a.exported_active_energy_kwh,
    a.imported_active_energy_kwh,
    a.exported_reactive_energy_kvarh,
    a.imported_reactive_energy_kvarh,

    -- Correntes
    a.current_phase_a_a,
    a.current_phase_b_a,
    a.current_phase_c_a,

    -- Tensões
    a.line_voltage_ab_v,
    a.line_voltage_bc_v,
    a.line_voltage_ca_v

from {{ ref('stg_meter_analog') }} a
join device d
  on d.id = a.device_id
 and d.is_active = true
join device_type dt
  on dt.id = d.device_type_id
join power_plant pp
  on pp.id = a.power_plant_id
 and pp.is_active = true
