{{ config(materialized = 'view') }}

with latest as (

    select
        a.*,
        row_number() over (
            partition by a.power_plant_id
            order by a.timestamp desc
        ) as rn
    from {{ ref('stg_weather_station_analog') }} a
)

select
    l.timestamp,
    l.power_plant_id,
    coalesce(pp.name, 'UNKNOWN') as power_plant_name,

    l.device_id,
    coalesce(d.name, 'UNKNOWN')  as device_name,
    coalesce(dt.name, 'weather') as device_type,

    -- Irradiância
    l.irradiance_ghi_wm2,
    l.irradiance_poa_wm2,

    -- Temperaturas
    l.air_temperature_c,
    l.module_temperature_c,

    -- Chuva
    l.rain_signal,
    l.accumulated_rain_mm,
    l.hourly_accumulated_rain_mm

from latest l
left join device d
  on d.id = l.device_id
left join device_type dt
  on dt.id = d.device_type_id
left join power_plant pp
  on pp.id = l.power_plant_id
where l.rn = 1
