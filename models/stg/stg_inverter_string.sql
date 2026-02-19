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

        -- jsonb -> text e cast seguro pra numeric
        case
          when nullif(trim(kv.value #>> '{}'), '') is null then null
          when trim(kv.value #>> '{}') ~ '^-?[0-9]+(\.[0-9]+)?$'
            then (trim(kv.value #>> '{}'))::numeric
          else null
        end as string_current

    from {{ source('public', 'raw_inverter') }} r
    cross join lateral jsonb_each(r.json_data) kv
    where kv.key like 'string_current_%'

    {% if is_incremental() %}
      and r.timestamp >= (
        select coalesce(max(timestamp), '1970-01-01') - interval '5 minutes'
        from {{ this }}
      )
    {% endif %}

),

parsed as (

    select
        timestamp,
        power_plant_id,
        device_id,

        -- extrai índice do nome da key
        regexp_replace(key, '[^0-9]', '', 'g')::int as string_index,

        string_current
    from base
    -- evita key sem índice (proteção)
    where regexp_replace(key, '[^0-9]', '', 'g') <> ''

)

select *
from parsed
