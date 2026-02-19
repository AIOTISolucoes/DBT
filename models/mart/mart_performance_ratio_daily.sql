{{ config(
    materialized = 'table'
) }}

with energy as (

    select
        power_plant_id,
        power_plant_name,
        date,
        energy_kwh
    from {{ ref('mart_energy_daily') }}
),

weather as (

    select
        power_plant_id,
        power_plant_name,
        date,

        -- Irradiância diária integrada (kWh/m²)
        (irradiance_poa_avg_wm2 * 24.0 / 1000.0)
            as irradiance_kwh_m2

    from {{ ref('mart_weather_daily') }}
),

-- 🔑 CAPACIDADE VINDO DA SEED (DADO MESTRE)
capacity as (

    select
        p.id as power_plant_id,
        c.capacity_dc
    from {{ source('public', 'power_plant') }} p
    join {{ ref('power_plant_capacity') }} c
      on c.power_plant_name = p.name
    where p.is_active = true
)

select
    e.power_plant_id,
    e.power_plant_name,
    e.date,

    -- Energia real medida
    e.energy_kwh as energy_real_kwh,

    -- Clima
    w.irradiance_kwh_m2,
    c.capacity_dc,

    -- Energia teórica
    (w.irradiance_kwh_m2 * c.capacity_dc)
        as energy_theoretical_kwh,

    -- PR físico (protegido)
    case
        when c.capacity_dc > 0
         and w.irradiance_kwh_m2 > 0
        then
            round(
                e.energy_kwh
                / (w.irradiance_kwh_m2 * c.capacity_dc),
                4
            )
        else null
    end as performance_ratio

from energy e
join weather w
  on w.power_plant_id = e.power_plant_id
 and w.date           = e.date
join capacity c
  on c.power_plant_id = e.power_plant_id
