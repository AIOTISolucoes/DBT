{{ config(
    materialized = 'table'
) }}

with devices as (

    -- 🔹 Todos os dispositivos ativos
    select
        d.id               as device_id,
        d.name             as device_name,
        dt.name            as device_type,

        p.id               as power_plant_id,
        p.name             as power_plant_name
    from {{ source('public', 'device') }} d
    join {{ source('public', 'device_type') }} dt
      on dt.id = d.device_type_id
    join {{ source('public', 'power_plant') }} p
      on p.id = d.power_plant_id
    where d.is_active = true
),

calendar as (

    -- 🔹 Calendário diário baseado em TODOS os eventos (não só alarmes)
    select distinct
        date(timestamp) as date
    from {{ ref('int_alarm_event') }}

    union

    -- 🔹 Garante o dia atual mesmo sem eventos
    select current_date
),

alarm_events as (

    -- 🔹 Alarmes críticos / altos (downtime)
    -- ✅ event_value agora é TEXT -> tratamos como "ativo" se for 1/true/t
    select
        power_plant_id,
        device_id,
        device_type,
        date(timestamp) as date,
        count(*)        as downtime_minutes
    from {{ ref('int_alarm_event') }}
    where
        event_type = 'alarm'
        and severity in ('critical', 'high')
        and coalesce(nullif(event_value, ''), '0') in ('1','true','TRUE','t','T')
    group by
        power_plant_id,
        device_id,
        device_type,
        date
),

base as (

    select
        d.power_plant_id,
        d.power_plant_name,

        d.device_id,
        d.device_name,
        d.device_type,

        c.date,

        coalesce(a.downtime_minutes, 0) as downtime_minutes
    from devices d
    cross join calendar c
    left join alarm_events a
      on a.device_id = d.device_id
     and a.date = c.date
)

select
    power_plant_id,
    power_plant_name,

    device_id,
    device_name,
    device_type,

    date,

    1440                               as total_minutes,
    downtime_minutes,
    (1440 - downtime_minutes)          as uptime_minutes,

    round(
        (1440 - downtime_minutes)::numeric / 1440,
        4
    )                                  as availability_pct

from base
