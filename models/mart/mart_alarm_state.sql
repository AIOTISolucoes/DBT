{{ config(materialized = 'view') }}

select
    a.id as alarm_id,

    pp.customer_id,

    a.power_plant_id,
    pp.name as power_plant_name,

    a.device_id,
    d.name  as device_name,
    dt.name as device_type,

    a.event_code,

    -- Nome humano do catálogo (fallback pra "Evento X")
    coalesce(ec.name, 'Evento ' || a.event_code::text) as event_name,

    -- Severidade do catálogo (fallback pra low)
    coalesce(ec.severity, 'low') as severity,

    a.state as alarm_state,

    -- timestamps oficiais de estado
    a.started_at,
    a.ack_at,
    a.cleared_at,
    a.created_at,

    -- último “timestamp de estado” útil pra ordenação
    coalesce(a.cleared_at, a.ack_at, a.started_at, a.created_at) as last_event_ts

from public.alarm_state a
join public.power_plant pp
  on pp.id = a.power_plant_id
join public.device d
  on d.id = a.device_id
join public.device_type dt
  on dt.id = d.device_type_id
left join public.event_catalog ec
  on ec.code = a.event_code
 and ec.device_type_id = d.device_type_id
