{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'device_id'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        r.json_data
    from {{ source('public', 'raw_transformer') }} r
    inner join public.device d
        on d.id = r.device_id
       and d.is_active is true

    {% if is_incremental() %}
      where r.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

normalized as (

    select
        timestamp,
        power_plant_id,
        device_id,
        coalesce(
            json_data -> 'discrete_data',
            json_data -> 'discrete',
            json_data -> 'discreteData',
            json_data
        ) as discrete_src
    from base

),

current_values as (

    select
        n.timestamp,
        n.power_plant_id,
        n.device_id,
        kv.key,
        kv.value
    from normalized n
    cross join lateral jsonb_each(n.discrete_src) kv
    where kv.key in (
        'communication_fault',
        'sensor_fault',
        'alarm_general',
        'trip_general',
        'oil_temp_alarm',
        'oil_temp_trip',
        'winding_temp_alarm',
        'winding_temp_trip',
        'buchholz_alarm',
        'buchholz_trip',
        'pressure_relief_trip',
        'oil_level_low',
        'temp_alarm_s1',
        'temp_alarm_s2',
        'temp_alarm_s3',
        'temp_trip_s1',
        'temp_trip_s2',
        'temp_trip_s3',
        'fan_on',
        'fan_fault'
    )

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