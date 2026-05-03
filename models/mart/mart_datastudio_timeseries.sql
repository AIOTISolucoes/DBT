{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['customer_id', 'power_plant_id', 'pathname', 'ts'],
    on_schema_change='append_new_columns',
    post_hook=[
      "create index if not exists ix_ds_ts_cust_plant_path_ts on {{ this }} (customer_id, power_plant_id, pathname, ts)"
    ]
) }}

select *
from {{ ref('int_datastudio_points') }}
where value is not null
{% if is_incremental() %}
  and ts >= (SELECT max(ts) - interval '3 days' FROM {{ this }})
{% endif %}