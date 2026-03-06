{{ config(materialized = 'view') }}

select
    r.timestamp,
    r.power_plant_id,
    coalesce(pp.name, 'UNKNOWN') as power_plant_name,

    r.device_id,
    coalesce(d.name, 'UNKNOWN')  as device_name,
    coalesce(dt.name, 'weather') as device_type,

    -- Irradiância
    r.irradiance_ghi_wm2,
    r.irradiance_poa_wm2,

    -- Temperaturas
    r.air_temperature_c,
    r.module_temperature_c,

    -- Chuva
    r.rain_signal,
    r.accumulated_rain_mm,
    r.hourly_accumulated_rain_mm

from {{ ref('stg_weather_station_analog') }} r
left join device d
  on d.id = r.device_id
left join device_type dt
  on dt.id = d.device_type_id
left join power_plant pp
  on pp.id = r.power_plant_id
