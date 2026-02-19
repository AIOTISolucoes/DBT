{{ config(
    materialized = 'table'
) }}

with inverter_5min as (

    select
        power_plant_id,
        date_trunc('minute', timestamp)
            - interval '1 minute' * (extract(minute from timestamp)::int % 5)
            as ts_5min,

        sum(active_power_kw) as active_power_kw
    from {{ ref('int_inverter_analog') }}
    where active_power_kw is not null
    group by
        power_plant_id,
        ts_5min
),

weather_5min as (

    select
        power_plant_id,
        date_trunc('minute', timestamp)
            - interval '1 minute' * (extract(minute from timestamp)::int % 5)
            as ts_5min,

        avg(irradiance_ghi_wm2) as irradiance_ghi_wm2,
        avg(irradiance_poa_wm2) as irradiance_poa_wm2
    from {{ ref('int_weather_station_analog') }}
    where irradiance_ghi_wm2 is not null
    group by
        power_plant_id,
        ts_5min
)

select
    i.power_plant_id,
    i.ts_5min                         as ts,

    i.active_power_kw,
    w.irradiance_ghi_wm2,
    w.irradiance_poa_wm2
from inverter_5min i
left join weather_5min w
  on w.power_plant_id = i.power_plant_id
 and w.ts_5min = i.ts_5min
order by
    i.power_plant_id,
    ts
