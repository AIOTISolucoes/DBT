{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        r.json_data
    from {{ source('public','raw_transformer') }} r
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

parsed as (

    select
        timestamp,
        power_plant_id,
        device_id,

        case
            when trim(json_data ->> 'temp_max_s1') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_max_s1')::numeric
            else null
        end as temp_max_s1,

        case
            when trim(json_data ->> 'temp_atual_s1') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_atual_s1')::numeric
            else null
        end as temp_atual_s1,

        case
            when trim(json_data ->> 'temp_max_s2') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_max_s2')::numeric
            else null
        end as temp_max_s2,

        case
            when trim(json_data ->> 'temp_atual_s2') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_atual_s2')::numeric
            else null
        end as temp_atual_s2,

        case
            when trim(json_data ->> 'temp_max_s3') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_max_s3')::numeric
            else null
        end as temp_max_s3,

        case
            when trim(json_data ->> 'temp_atual_s3') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_atual_s3')::numeric
            else null
        end as temp_atual_s3,

        case
            when trim(json_data ->> 'temp_max_amb') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_max_amb')::numeric
            else null
        end as temp_max_amb,

        case
            when trim(json_data ->> 'temp_atual_amb') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_atual_amb')::numeric
            else null
        end as temp_atual_amb,

        case
            when trim(json_data ->> 'temp_top_oil') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_top_oil')::numeric
            else null
        end as temp_top_oil,

        case
            when trim(json_data ->> 'temp_bottom_oil') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_bottom_oil')::numeric
            else null
        end as temp_bottom_oil,

        case
            when trim(json_data ->> 'temp_winding') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'temp_winding')::numeric
            else null
        end as temp_winding,

        case
            when trim(json_data ->> 'oil_level_pct') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'oil_level_pct')::numeric
            else null
        end as oil_level_pct,

        case
            when trim(json_data ->> 'oil_pressure') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'oil_pressure')::numeric
            else null
        end as oil_pressure,

        case
            when trim(json_data ->> 'load_current') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'load_current')::numeric
            else null
        end as load_current,

        case
            when trim(json_data ->> 'moisture_in_oil') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'moisture_in_oil')::numeric
            else null
        end as moisture_in_oil,

        case
            when trim(json_data ->> 'dga_total_gas') ~ '^-?\d+(\.\d+)?$'
                then trim(json_data ->> 'dga_total_gas')::numeric
            else null
        end as dga_total_gas

    from base

)

select *
from parsed