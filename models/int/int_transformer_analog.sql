{{ config(materialized = 'view') }}

select
    a.timestamp,
    a.power_plant_id,
    pp.name            as power_plant_name,

    a.device_id,
    d.name             as device_name,
    dt.name            as device_type

    -- Campos analógicos do transformador
    -- A DEFINIR (ex: tensões, correntes, temperatura, taps, óleo)

from {{ ref('stg_transformer_analog') }} a
join device d
  on d.id = a.device_id
 and d.is_active = true
join device_type dt
  on dt.id = d.device_type_id
join power_plant pp
  on pp.id = a.power_plant_id
 and pp.is_active = true
