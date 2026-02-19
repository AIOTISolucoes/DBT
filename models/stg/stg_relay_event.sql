{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id','event_code']
) }}

with base as (

    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        kv.key,
        kv.value::boolean as event_value
    from {{ source('public','raw_relay') }} r
    cross join lateral jsonb_each(r.json_data) kv
    where
        jsonb_typeof(kv.value) = 'boolean'
        and kv.key not like '%_quality'
        and (
            kv.key like 'flag_%'
            or kv.key in (
                'trip_circuit_fail',
                'i2t_accumulator_status',
                'relay_synchronism_status'
            )
        )

    {% if is_incremental() %}
      and r.timestamp > (select max(timestamp) from {{ this }})
    {% endif %}

),

parsed as (

    select
        timestamp,
        power_plant_id,
        device_id,

        -- extrai SOMENTE o número do flag
        regexp_replace(key, '[^0-9]', '', 'g')::int as event_code,
        event_value
    from base
    where regexp_replace(key, '[^0-9]', '', 'g') <> ''

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
    from parsed
)

select
    timestamp,
    power_plant_id,
    device_id,
    event_code,
    event_value
from dedup
where rn = 1
