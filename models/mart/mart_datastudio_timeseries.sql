{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_ts_cust_plant_path_ts on {{ this }} (customer_id, power_plant_id, pathname, ts)"
    ]
) }}

select *
from {{ ref('int_datastudio_points') }}
where value is not null