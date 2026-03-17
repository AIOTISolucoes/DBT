{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_monthly_cust_plant_month on {{ this }} (customer_id, power_plant_id, month)"
    ]
) }}

with plants as (
  select
    id as power_plant_id,
    customer_id
  from {{ source('public','power_plant') }}
),

m as (
  select
    power_plant_id,
    month,
    energy_kwh
  from {{ ref('mart_energy_monthly') }}
)

select
  p.customer_id,
  m.power_plant_id,
  m.month,
  m.energy_kwh
from m
join plants p
  on p.power_plant_id = m.power_plant_id