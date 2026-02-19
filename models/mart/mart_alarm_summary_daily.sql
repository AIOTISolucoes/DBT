{{ config(
    materialized = 'table'
) }}

with base as (

    select
        power_plant_id,
        power_plant_name,
        date(timestamp) as date,
        event_type,
        severity
    from {{ ref('int_alarm_event') }}
),

agg as (

    select
        power_plant_id,
        power_plant_name,
        date,

        count(*)                                      as total_events,
        count(*) filter (where event_type = 'alarm') as total_alarms,

        count(*) filter (where severity = 'critical') as critical_alarms,
        count(*) filter (where severity = 'high')     as high_alarms,
        count(*) filter (where severity = 'medium')   as medium_alarms,
        count(*) filter (where severity = 'low')      as low_alarms

    from base
    group by
        power_plant_id,
        power_plant_name,
        date
)

select *
from agg
