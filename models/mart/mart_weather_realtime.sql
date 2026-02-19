{{ config(materialized = 'view') }}

with ranked as (

    select
        *,
        row_number() over (
            partition by power_plant_id
            order by
                -- prioriza registros com irradiância válida
                (irradiance_ghi_wm2 is not null)::int desc,
                timestamp desc
        ) as rn
    from {{ ref('int_weather_station_analog') }}
)

select
    power_plant_id,
    power_plant_name,

    timestamp as last_update,

    irradiance_ghi_wm2,
    irradiance_poa_wm2,
    air_temperature_c,
    module_temperature_c,

    rain_signal

from ranked
where rn = 1
