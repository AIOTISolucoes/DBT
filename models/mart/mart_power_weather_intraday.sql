{{ config(materialized = 'table') }}

with base as (

    select
        ia.power_plant_id,
        ia.device_id,
        ia.timestamp,
        ia.active_power_kw,

        -- regra do projeto
        ia.communication_fault_code,
        ia.is_communication_ok,

        -- bin 5 min (UTC)
        date_bin(
          '5 minutes',
          ia.timestamp,
          timestamptz '2000-01-01 00:00:00+00'
        ) as ts_5min

    from {{ ref('int_inverter_analog') }} ia
    where ia.active_power_kw is not null
),

dedup as (
    -- 1 linha por inverter por bin (pega a leitura mais recente dentro do bin)
    select *
    from (
        select
            b.*,
            row_number() over (
                partition by b.power_plant_id, b.device_id, b.ts_5min
                order by b.timestamp desc
            ) as rn
        from base b
    ) x
    where rn = 1
),

inverter_5min as (

    select
        power_plant_id,
        ts_5min,

        -- soma APENAS OK (192)
        sum(active_power_kw) filter (
          where is_communication_ok = 1
             or communication_fault_code = 192
        ) as active_power_kw,

        -- contadores (debug/auditoria)
        count(*) filter (
          where is_communication_ok = 1
             or communication_fault_code = 192
        ) as inverter_count_ok,

        count(*) filter (
          where communication_fault_code = 28
             or is_communication_ok = 0
        ) as inverter_count_fault,

        count(*) filter (
          where communication_fault_code is null
           and is_communication_ok is null
        ) as inverter_count_null

    from dedup
    group by 1,2
),

weather_5min as (

    select
        power_plant_id,
        date_bin(
          '5 minutes',
          timestamp,
          timestamptz '2000-01-01 00:00:00+00'
        ) as ts_5min,

        avg(irradiance_ghi_wm2) as irradiance_ghi_wm2,
        avg(irradiance_poa_wm2) as irradiance_poa_wm2
    from {{ ref('int_weather_station_analog') }}
    where irradiance_ghi_wm2 is not null
    group by 1,2
)

select
    i.power_plant_id,
    i.ts_5min as ts,

    coalesce(i.active_power_kw, 0) as active_power_kw,
    w.irradiance_ghi_wm2,
    w.irradiance_poa_wm2,

    -- debug/auditoria
    i.inverter_count_ok,
    i.inverter_count_fault,
    i.inverter_count_null

from inverter_5min i
left join weather_5min w
  on w.power_plant_id = i.power_plant_id
 and w.ts_5min = i.ts_5min

order by
    i.power_plant_id,
    ts