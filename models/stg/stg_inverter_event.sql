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
    from {{ source('public', 'raw_inverter') }} r

    {% if is_incremental() %}
      where r.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}

),

-- tenta achar "sub-objeto" de discretos (se existir); senão usa o json inteiro
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

discrete_only as (

    select
        timestamp,
        power_plant_id,
        device_id,
        coalesce(
            jsonb_object_agg(key, value),
            '{}'::jsonb
        ) as discrete_data_json
    from (
        select
            timestamp,
            power_plant_id,
            device_id,
            key,
            value
        from normalized
        cross join lateral jsonb_each(discrete_src)
        where
            lower(key) like 'id%'
            or key in ('communication_fault', 'working_status', 'inverter_status')
    ) d
    group by
        timestamp,
        power_plant_id,
        device_id
)

select *
from discrete_only
