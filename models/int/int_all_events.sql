{{ config(
    materialized = 'incremental',
    unique_key = 'event_union_row_id',
    on_schema_change = 'sync_all_columns'
) }}

with unioned as (

    select
        'inverter'::text as event_source,
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('stg_inverter_event') }} e
    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
        where event_source = 'inverter'
      )
    {% endif %}

    union all

    select
        'relay'::text as event_source,
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('stg_relay_event') }} e
    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
        where event_source = 'relay'
      )
    {% endif %}

    union all

    select
        'tracker'::text as event_source,
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('stg_tracker_event') }} e
    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
        where event_source = 'tracker'
      )
    {% endif %}

    union all

    select
        'transformer'::text as event_source,
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('stg_transformer_event') }} e
    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
        where event_source = 'transformer'
      )
    {% endif %}

    union all

    select
        'meter'::text as event_source,
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('stg_meter_event') }} e
    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
        where event_source = 'meter'
      )
    {% endif %}

    union all

    select
        'weather_station'::text as event_source,
        e.timestamp,
        e.power_plant_id,
        e.device_id,
        e.discrete_data_json::jsonb as discrete_data_json
    from {{ ref('stg_weather_station_event') }} e
    {% if is_incremental() %}
      where e.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
        where event_source = 'weather_station'
      )
    {% endif %}

),

final as (

    select
        md5(
            coalesce(event_source, '') || '|' ||
            coalesce(timestamp::text, '') || '|' ||
            coalesce(device_id::text, '') || '|' ||
            coalesce(discrete_data_json::text, '')
        ) as event_union_row_id,
        event_source,
        timestamp,
        power_plant_id,
        device_id,
        discrete_data_json
    from unioned

)

select *
from final