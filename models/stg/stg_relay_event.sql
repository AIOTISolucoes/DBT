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
    from {{ source('public', 'raw_relay') }} r
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
        kv.value::text as value
    from normalized n
    cross join lateral jsonb_each_text(n.discrete_src) kv
    where
        kv.key in (
            'communication_fault',
            'flag_46',
            'flag_50',
            'flag_51_1',
            'flag_50N',
            'flag_51GS',
            'flag_51N',
            'flag_27',
            'flag_59',
            'flag_47',
            'flag_81_O',
            'flag_81_U',
            'flag_51_2',
            'status_relay'
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
        jsonb_object_agg(key, to_jsonb(value)) as discrete_data_json
    from only_changed_keys
    group by
        timestamp,
        power_plant_id,
        device_id

)

select *
from changed_json