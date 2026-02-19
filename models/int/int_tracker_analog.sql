{{ config(materialized = 'view') }}

select
    a.timestamp,
    a.power_plant_id,
    pp.name            as power_plant_name,

    a.device_id,
    d.name             as device_name,
    dt.name            as device_type,

    -- Vínculo técnico
    a.inverter_id,
    inv.name           as inverter_name,

    -- Identificação do tracker
    a.serial_number,
    a.manufacturer,

    -- Métricas analógicas
    a.angular_position_current_deg,
    a.angular_position_target_deg

from {{ ref('stg_tracker_analog') }} a
join device d
  on d.id = a.device_id
 and d.is_active = true
join device_type dt
  on dt.id = d.device_type_id
left join device inv
  on inv.id = a.inverter_id
join power_plant pp
  on pp.id = a.power_plant_id
 and pp.is_active = true
