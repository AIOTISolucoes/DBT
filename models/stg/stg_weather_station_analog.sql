{{ config(materialized = 'table') }}

select
    r.timestamp,
    r.power_plant_id,
    r.device_id,

    -- ✅ Irradiância (keys reais do payload)
    NULLIF(r.json_data ->> 'GHI_irradiance', '')::numeric  as irradiance_ghi_wm2,
    NULLIF(r.json_data ->> 'POA_irradiance', '')::numeric  as irradiance_poa_wm2,

    -- ✅ Temperaturas (keys reais do payload)
    NULLIF(r.json_data ->> 'temp_ambiente', '')::numeric   as air_temperature_c,
    NULLIF(r.json_data ->> 'temp_modulo', '')::numeric     as module_temperature_c,

    -- ✅ Chuva (keys reais do payload)
    NULLIF(r.json_data ->> 'sensor_chuva', '')::numeric    as rain_signal,
    NULLIF(r.json_data ->> 'hourly_accumulated_rain', '')::numeric as hourly_accumulated_rain_mm,
    NULLIF(r.json_data ->> 'acumulador_pluv_acc', '')::numeric as accumulated_rain_mm,

    -- ✅ Qualidade/saúde da estação (192 ok, 28 falha)
    NULLIF(r.json_data ->> 'communication_fault', '')::int as communication_fault,

    -- extras úteis (opcional)
    NULLIF(r.json_data ->> 'wind_speed', '')::numeric      as wind_speed,
    NULLIF(r.json_data ->> 'wind_direction', '')::numeric  as wind_direction,
    NULLIF(r.json_data ->> 'volt_battery', '')::numeric    as volt_battery

from {{ source('public','raw_weather_station') }} r

-- ✅ opcional: descartar leituras com falha de comunicação (28)
-- where COALESCE(NULLIF(r.json_data ->> 'communication_fault','')::int, 192) <> 28
