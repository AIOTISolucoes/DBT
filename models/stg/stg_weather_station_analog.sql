{{ config(
    materialized = 'table'
) }}

select
    r.timestamp,
    r.power_plant_id,
    r.device_id,

    -- Irradiância
    (r.json_data ->> 'SR05_GHI_irradiance')::numeric  as irradiance_ghi_wm2,
    (r.json_data ->> 'SR05_POA_irradiance')::numeric  as irradiance_poa_wm2,

    -- Temperaturas
    (r.json_data ->> 'SSTRH_temperatura')::numeric    as air_temperature_c,
    (r.json_data ->> 'pt1000_painel1')::numeric       as module_temperature_c,

    -- Chuva
    (r.json_data ->> 'acumulador_pluv_minute')::numeric as rain_signal,
    (r.json_data ->> 'acumulador_pluv_hour')::numeric  as hourly_accumulated_rain_mm,
    (r.json_data ->> 'acumulador_pluv_day')::numeric   as accumulated_rain_mm

from {{ source('public','raw_weather_station') }} r
