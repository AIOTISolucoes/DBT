{{ config(
    materialized = 'table'
) }}

select
    power_plant_id,
    power_plant_name,
    date(timestamp) as date,

    avg(irradiance_ghi_wm2)         as irradiance_ghi_avg_wm2,
    avg(irradiance_poa_wm2)         as irradiance_poa_avg_wm2,

    max(irradiance_ghi_wm2)         as irradiance_ghi_max_wm2,
    max(irradiance_poa_wm2)         as irradiance_poa_max_wm2,

    avg(air_temperature_c)          as air_temperature_avg_c,
    avg(module_temperature_c)       as module_temperature_avg_c,

    sum(hourly_accumulated_rain_mm) as rain_accumulated_mm

from {{ ref('int_weather_station_analog') }}
group by
    power_plant_id,
    power_plant_name,
    date
