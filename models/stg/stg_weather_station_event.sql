{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        w.timestamp,
        w.power_plant_id,
        w.device_id,
        w.json_data
    from {{ source('public','raw_weather_station') }} w
    inner join public.device d
        on d.id = w.device_id
       and d.is_active is true

    {% if is_incremental() %}
      where w.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

current_values as (

    select
        timestamp,
        power_plant_id,
        device_id,
        'communication_fault' as key,
        json_data -> 'communication_fault' as value
    from base
    where json_data ? 'communication_fault'

),

with_prev as (

    select
        *,
        lag(value) over (
            partition by power_plant_id, device_id, key
            order by timestamp
        ) as prev_value
    from current_values

),

only_changed_keys as (

    select
        timestamp,
        power_plant_id,
        device_id,
        key,
        value
    from with_prev
    where prev_value is distinct from value

),

changed_json as (

    select
        timestamp,
        power_plant_id,
        device_id,
        jsonb_object_agg(key, value) as discrete_data_json
    from only_changed_keys
    group by
        timestamp,
        power_plant_id,
        device_id

)

select *
from changed_json