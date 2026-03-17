{{ config(
    materialized='table',
    post_hook=[
      "create index if not exists ix_ds_daily_cust_plant_date on {{ this }} (customer_id, power_plant_id, date)"
    ]
) }}

with plants as (

    select
        id as power_plant_id,
        customer_id
    from {{ source('public','power_plant') }}

),

pr as (

    select
        power_plant_id,
        date,
        energy_real_kwh,
        irradiance_kwh_m2,
        capacity_dc,
        energy_theoretical_kwh,
        performance_ratio
    from {{ ref('mart_performance_ratio_daily') }}

)

select
    p.customer_id,
    pr.power_plant_id,
    pr.date,

    -- métricas do Data Studio
    pr.energy_real_kwh,
    pr.irradiance_kwh_m2,
    pr.energy_theoretical_kwh,
    pr.performance_ratio,

    -- referência
    pr.capacity_dc

from pr
join plants p
  on p.power_plant_id = pr.power_plant_id