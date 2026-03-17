{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_intraday_cust_plant_ts on {{ this }} (customer_id, power_plant_id, ts)"
    ]
) }}

with plants as (
  select
    id as power_plant_id,
    customer_id
  from {{ source('public','power_plant') }}
),

base as (
  select
    power_plant_id,
    ts,
    active_power_kw,
    irradiance_ghi_wm2,
    irradiance_poa_wm2,
    inverter_count_ok,
    inverter_count_fault,
    inverter_count_null
  from {{ ref('mart_power_weather_intraday') }}
)

select
  p.customer_id,
  b.*
from base b
join plants p
  on p.power_plant_id = b.power_plant_id