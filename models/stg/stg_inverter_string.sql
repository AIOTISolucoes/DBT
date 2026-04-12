{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id','string_index'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        kv.key,

        case
          when nullif(trim(kv.value #>> '{}'), '') is null then null
          when trim(kv.value #>> '{}') ~ '^-?[0-9]+(\.[0-9]+)?$'
            then (trim(kv.value #>> '{}'))::numeric
          else null
        end as string_current

    from {{ source('public', 'raw_inverter') }} r
    inner join public.device d
        on r.device_id = d.id
    cross join lateral jsonb_each(r.json_data::jsonb) kv
    where coalesce(d.is_active, false) = true
      and kv.key like 'string_current_%'

    {% if is_incremental() %}
      and r.timestamp >= (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz) - interval '5 minutes'
        from {{ this }}
      )
    {% endif %}

),

parsed as (

    select
        timestamp,
        power_plant_id,
        device_id,
        regexp_replace(key, '[^0-9]', '', 'g')::int as string_index,
        string_current
    from base
    where regexp_replace(key, '[^0-9]', '', 'g') <> ''

)

select *
from parsed