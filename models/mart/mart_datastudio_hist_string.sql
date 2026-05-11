{{ config(
    materialized='incremental',
    unique_key=['customer_id', 'power_plant_id', 'device_id', 'string_index', 'ts'],
    on_schema_change='append_new_columns',
    post_hook=[
      "create index if not exists ix_ds_hist_string on {{ this }} (customer_id, power_plant_id, device_id, string_index, ts)"
    ]
) }}

with source as (

    select
        pp.customer_id,
        s.power_plant_id,
        s.device_id,
        s.string_index,
        (
            date_trunc('hour', s.timestamp)
            + floor(date_part('minute', s.timestamp) / 15) * interval '15 minute'
        )::timestamptz as ts,
        avg(s.string_current)::double precision  as avg_value,
        max(s.string_current)::double precision  as max_value,
        sum(s.string_current)::double precision  as sum_value
    from rt.stg_inverter_string s
    join public.power_plant pp
      on pp.id = s.power_plant_id
    where s.string_current is not null
      and s.string_index   is not null

    {% if is_incremental() %}
      and s.timestamp > (
          select coalesce(max(ts), '1970-01-01'::timestamptz)
          from {{ this }}
      )
    {% endif %}

    group by
        pp.customer_id,
        s.power_plant_id,
        s.device_id,
        s.string_index,
        ts

)

select * from source
