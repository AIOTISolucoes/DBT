{{ config(
    materialized = 'incremental',
    unique_key = ['event_ts', 'device_id', 'event_code', 'source'],
    on_schema_change = 'sync_all_columns'
) }}

-- =====================================================
-- INVERTER EVENTS
-- =====================================================
select
    i.timestamp           as event_ts,
    i.power_plant_id,
    p.customer_id,                -- 🔹 MULTI-CLIENTE
    i.device_id,

    'inverter'            as equipment,
    'inverter'            as device_type,   -- ✅ extra p/ padronizar UI

    i.event_code,
    i.event_name,
    i.event_type,
    i.severity,
    i.event_value::text   as event_value,   -- ✅ FIX: padroniza tipo

    'inverter'            as source

from {{ ref('int_inverter_event') }} i
join {{ source('public', 'power_plant') }} p
  on p.id = i.power_plant_id

{% if is_incremental() %}
where i.timestamp > (
    select coalesce(max(event_ts), '1970-01-01')
    from {{ this }}
    where source = 'inverter'
)
{% endif %}

union all

-- =====================================================
-- RELAY EVENTS
-- =====================================================
select
    r.timestamp           as event_ts,
    r.power_plant_id,
    p.customer_id,                -- 🔹 MULTI-CLIENTE
    r.device_id,

    'relay'               as equipment,
    'relay'               as device_type,

    r.event_code,
    r.event_name,
    r.event_type,
    r.severity,
    r.event_value::text   as event_value,   -- ✅ FIX

    'relay'               as source

from {{ ref('int_relay_event') }} r
join {{ source('public', 'power_plant') }} p
  on p.id = r.power_plant_id

{% if is_incremental() %}
where r.timestamp > (
    select coalesce(max(event_ts), '1970-01-01')
    from {{ this }}
    where source = 'relay'
)
{% endif %}

union all

-- =====================================================
-- WEATHER STATION EVENTS
-- =====================================================
select
    w.timestamp           as event_ts,
    w.power_plant_id,
    p.customer_id,
    w.device_id,

    'weather'             as equipment,
    'weather'             as device_type,

    w.event_code,
    w.event_name,
    w.event_type,
    w.severity,
    w.event_value::text   as event_value,   -- ✅ FIX

    'weather'             as source

from {{ ref('int_weather_station_event') }} w
join {{ source('public', 'power_plant') }} p
  on p.id = w.power_plant_id

{% if is_incremental() %}
where w.timestamp > (
    select coalesce(max(event_ts), '1970-01-01')
    from {{ this }}
    where source = 'weather'
)
{% endif %}
