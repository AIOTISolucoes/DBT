{{ config(
    materialized = 'incremental',
    unique_key = 'alarm_id'
) }}

select
    a.id as alarm_id,

    pp.customer_id,                 -- ✅ MULTI-TENANT
    a.power_plant_id,
    pp.name as power_plant_name,

    a.device_id,
    d.name  as device_name,
    dt.name as device_type,

    a.event_code,
    ec.name     as event_name,
    ec.severity as severity,

    a.state     as alarm_state,

    a.started_at,
    a.ack_at,
    a.cleared_at,
    a.created_at,

    case
        when a.cleared_at is not null then
            extract(epoch from (a.cleared_at - a.started_at))
    end as duration_seconds

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

{% if is_incremental() %}
where a.id > (select coalesce(max(alarm_id), 0) from {{ this }})
{% endif %}
