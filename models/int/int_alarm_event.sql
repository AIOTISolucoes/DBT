{{ config(materialized = 'view') }}

with union_events as (

    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_inverter_event') }}

    union all
    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_relay_event') }}

    union all
    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_logger_event') }}

    union all
    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_nobreak_event') }}

    union all
    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_tracker_event') }}

    union all
    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_transformer_event') }}

    union all
    select
      timestamp,
      power_plant_id,
      device_id,
      event_code,
      event_name,
      event_type,
      severity,
      event_value::text as event_value
    from {{ ref('int_weather_station_event') }}

),

enriched as (
    select
        u.timestamp,
        pp.customer_id,

        u.power_plant_id,
        pp.name as power_plant_name,

        u.device_id,
        d.name  as device_name,
        dt.name as device_type,

        u.event_code,
        u.event_name,
        u.event_type,
        u.severity,
        u.event_value

    from union_events u
    join {{ source('public', 'power_plant') }} pp
      on pp.id = u.power_plant_id
     and pp.is_active = true

    join {{ source('public', 'device') }} d
      on d.id = u.device_id
     and d.is_active = true

    join {{ source('public', 'device_type') }} dt
      on dt.id = d.device_type_id
)

select * from enriched