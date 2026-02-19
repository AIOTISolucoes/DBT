{{ config(
    materialized = 'table'
) }}

select
    power_plant_id,
    power_plant_name,
    date(timestamp) as date,

    max(exported_active_energy_kwh)
      - min(exported_active_energy_kwh)
        as exported_energy_kwh,

    max(imported_active_energy_kwh)
      - min(imported_active_energy_kwh)
        as imported_energy_kwh

from {{ ref('int_meter_analog') }}
group by
    power_plant_id,
    power_plant_name,
    date
