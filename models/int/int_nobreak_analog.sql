{{ config(materialized = 'view') }}

select
    a.timestamp,
    a.power_plant_id,
    pp.name            as power_plant_name,

    a.device_id,
    d.name             as device_name,
    dt.name            as device_type,

    -- Entrada
    a.input_voltage_v,
    a.input_frequency_hz,

    -- Saída
    a.output_voltage_v,
    a.output_frequency_hz,
    a.output_current_a,
    a.output_power_kw,
    a.output_apparent_power_kva,

    -- Bateria
    a.battery_voltage_v,
    a.battery_capacity_pct,
    a.battery_temperature_c,

    -- Carga / autonomia
    a.load_percentage_pct,
    a.remaining_backup_time_min

from {{ ref('stg_nobreak_analog') }} a
join device d
  on d.id = a.device_id
 and d.is_active = true
join device_type dt
  on dt.id = d.device_type_id
join power_plant pp
  on pp.id = a.power_plant_id
 and pp.is_active = true
