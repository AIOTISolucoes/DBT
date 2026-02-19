{{ config(
    materialized='incremental',
    unique_key=['timestamp','power_plant_id','device_id','event_code'],
    on_schema_change='sync_all_columns'
) }}

with exploded as (

    select
        s.timestamp,
        s.power_plant_id,
        s.device_id,

        -- event_code
        case
            when lower(kv.key) like 'id%'
              then regexp_replace(kv.key, '[^0-9]', '', 'g')::int

            when lower(kv.key) = 'inverter_status' then 9001
            when lower(kv.key) = 'working_status'  then 9002
            when lower(kv.key) = 'communication_fault' then 9003

            else null
        end as event_code,

        -- boolean apenas para IDs
        case
            when lower(kv.key) like 'id%' then
              case
                when lower(kv.value) in ('true','false')
                  then (lower(kv.value) = 'true')
                when kv.value ~ '^[0-9]+$'
                  then (kv.value::int <> 0)
                else false
              end
            else null
        end as event_value_bool,

        -- int apenas para status
        case
            when lower(kv.key) in ('inverter_status','working_status','communication_fault')
             and kv.value ~ '^[0-9]+$'
              then kv.value::int
            when lower(kv.key) in ('inverter_status','working_status','communication_fault')
             and lower(kv.value) in ('true','false')
              then (lower(kv.value) = 'true')::int
            else null
        end as event_value_int,

        kv.key   as raw_key,
        kv.value as raw_value

    from {{ ref('stg_inverter_event') }} s
    cross join lateral jsonb_each_text(s.discrete_data_json) kv
    where s.discrete_data_json is not null
),

-- ✅ marca se existe algum IDxx=1 no mesmo timestamp/device (mesmo payload)
has_id_alarm as (
    select
        timestamp,
        power_plant_id,
        device_id,
        true as has_id_alarm
    from exploded
    where lower(raw_key) like 'id%'
      and event_value_bool = true
    group by 1,2,3
),

catalog_join as (

    select
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.event_code,

        -- Nome
        case
          when lower(e.raw_key) like 'id%' then
            coalesce(nullif(c.name,''), concat('ID', e.event_code::text))

          when lower(e.raw_key) = 'inverter_status' then 'Status do inversor'
          when lower(e.raw_key) = 'working_status'  then 'Working status'
          when lower(e.raw_key) = 'communication_fault' then 'Communication fault'

          else concat('Evento ', e.event_code::text)
        end as event_name,

        -- ✅ status fixo SEMPRE status (9001/9002/9003 não viram alarm)
        case
          when e.event_code in (9001,9002,9003) then 'status'
          when lower(e.raw_key) like 'id%' then coalesce(nullif(c.event_type,''), 'alarm')
          else 'status'
        end as event_type,

        -- ✅ AJUSTE PEDIDO:
        -- 9003=28 SEM IDxx=1 => severity = 'medium'
        -- demais status => 'low'
        case
          when e.event_code = 9003
           and e.event_value_int = 28
           and coalesce(h.has_id_alarm, false) = false
          then 'medium'

          when e.event_code in (9001,9002,9003) then 'low'
          when lower(e.raw_key) like 'id%' then coalesce(nullif(c.severity,''), 'low')
          else 'low'
        end as severity,

        -- ✅ event_value sempre TEXT
        case
          when lower(e.raw_key) like 'id%' then
            case when e.event_value_bool then '1' else '0' end
          else
            e.event_value_int::text
        end as event_value,

        -- flags (só para IDs)
        case
          when lower(e.raw_key) like 'id%'
           and coalesce(c.event_type,'') = 'communication'
           and e.event_value_bool = false
          then true
          else false
        end as comm_lost,

        case
          when lower(e.raw_key) like 'id%'
           and coalesce(c.event_type,'') = 'fault'
           and e.event_value_bool = true
          then true
          else false
        end as fault_active,

        coalesce(h.has_id_alarm, false) as has_id_alarm,

        e.raw_key,
        e.raw_value

    from exploded e
    left join has_id_alarm h
      on h.timestamp = e.timestamp
     and h.power_plant_id = e.power_plant_id
     and h.device_id = e.device_id

    left join {{ source('public', 'event_catalog') }} c
      on lower(e.raw_key) like 'id%'
     and c.code = e.event_code
     and c.device_type_id = (
        select id
        from {{ source('public', 'device_type') }}
        where name = 'inverter'
     )

    where e.event_code is not null
      and (
        (lower(e.raw_key) like 'id%' and e.event_value_bool = true)
        or (lower(e.raw_key) not like 'id%')
      )
)

select
    timestamp,
    power_plant_id,
    device_id,
    event_code,
    event_name,
    event_type,
    severity,
    event_value,
    comm_lost,
    fault_active,
    raw_key,
    raw_value
from catalog_join

-- ✅ continua silenciando 9003=28 quando existir IDxx=1 no mesmo payload
where not (
    event_code = 9003
    and event_value = '28'
    and has_id_alarm = true
)

{% if is_incremental() %}
  and timestamp > (
      select coalesce(max(timestamp), '1970-01-01'::timestamptz)
      from {{ this }}
  )
{% endif %}
