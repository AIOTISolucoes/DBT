{{ config(materialized='view') }}

with me as (
  select *
  from {{ ref('mart_events') }}
),

enriched as (
  select
      me.customer_id,

      me.event_ts,
      me.power_plant_id,
      pp.name as power_plant_name,

      me.device_id,
      d.name as device_name,

      coalesce(nullif(me.device_type, ''), dt.name, 'unknown') as device_type,

      me.equipment,
      me.event_code,

      coalesce(nullif(me.event_type, ''), 'event') as event_type,

      me.event_name,
      me.severity,
      me.event_value,
      me.source
  from me
  left join {{ source('public', 'power_plant') }} pp
    on pp.id = me.power_plant_id
   and pp.customer_id = me.customer_id

  left join {{ source('public', 'device') }} d
    on d.id = me.device_id

  left join {{ source('public', 'device_type') }} dt
    on dt.id = d.device_type_id
)

select
    customer_id,
    event_ts,
    power_plant_id,
    power_plant_name,
    device_id,
    device_name,
    device_type,
    equipment,
    event_code,

    case
      when event_code = 9003 and event_value = '28' then
        'Falha de comunicação (28)'

      when lower(coalesce(severity,'')) in ('medium','high','critical') then
        coalesce(nullif(event_name, ''), 'Alarme ' || event_code::text)

      else
        'Desatuado'
    end as event_name,

    event_type,
    severity,
    event_value,
    source

from enriched
