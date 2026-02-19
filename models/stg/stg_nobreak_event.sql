{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id','event_code']
) }}

with base as (
    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        kv.key::int        as event_code,
        kv.value::boolean as event_value
    from {{ source('public','raw_nobreak') }} r
    cross join lateral jsonb_each(r.json_data) kv
    where jsonb_typeof(kv.value) = 'boolean'
    {% if is_incremental() %}
      and r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}
),

dedup as (
    select
        *,
        row_number() over (
            partition by
                timestamp,
                power_plant_id,
                device_id,
                event_code
            order by timestamp
        ) as rn
    from base
)

select
    timestamp,
    power_plant_id,
    device_id,
    event_code,
    event_value
from dedup
where rn = 1
