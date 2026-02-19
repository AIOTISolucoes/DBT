{{ config(materialized = 'view') }}

select
    e.timestamp,
    e.power_plant_id,
    pp.name              as power_plant_name,

    e.device_id,
    d.name               as device_name,
    dt.name              as device_type,

    e.event_code,
    c.name               as event_name,
    c.event_type,
    c.severity,

    e.event_value

from {{ ref('stg_logger_event') }} e
join device d
  on d.id = e.device_id
 and d.is_active = true
join device_type dt
  on dt.id = d.device_type_id
join event_catalog c
  on c.code = e.event_code
 and c.device_type_id = dt.id
 and c.is_active = true
join power_plant pp
  on pp.id = e.power_plant_id
 and pp.is_active = true
