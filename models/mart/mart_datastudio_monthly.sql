{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_monthly_cust_plant_month on {{ this }} (customer_id, power_plant_id, month)"
    ]
) }}

with monthly as (
  select
    f.customer_id,
    f.power_plant_id,
    date_trunc('month', f.date_day)::date as month,
    sum(f.generation_daily_kwh)::double precision as energy_kwh
  from rt.fct_power_plant_metrics_daily f
  where f.generation_daily_kwh is not null
  group by
    f.customer_id,
    f.power_plant_id,
    date_trunc('month', f.date_day)::date
)

select
  customer_id,
  power_plant_id,
  month,
  energy_kwh
from monthly