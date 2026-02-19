{{ config(
    materialized = 'table'
) }}

select
    power_plant_id,
    power_plant_name,
    date_trunc('month', date)::date as month,
    round(sum(energy_kwh), 3)       as energy_kwh
from {{ ref('mart_energy_daily') }}
group by
    power_plant_id,
    power_plant_name,
    month
