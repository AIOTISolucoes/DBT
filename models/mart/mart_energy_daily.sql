{{ config(materialized = 'table') }}

with base as (
    select
        power_plant_id,
        power_plant_name,
        device_id,

        -- timestamp original (timestamptz) vindo do PLC
        timestamp as ts_utc,

        -- timestamp em hora local para janela e data
        timezone('America/Fortaleza', timestamp) as ts_local,
        timezone('America/Fortaleza', timestamp)::date as date_local,

        active_power_kw,
        communication_fault,

        row_number() over (
            partition by power_plant_id, device_id, timestamp
            order by timestamp desc
        ) as rn
    from {{ ref('int_inverter_analog') }}
    where active_power_kw is not null
),

dedup as (
    select *
    from base
    where rn = 1
),

filtered as (
    select *
    from dedup
    where
        -- ✅ janela de geração em hora local
        ts_local::time between time '04:00:00' and time '19:00:00'

        -- ✅ comunicação OK
        and communication_fault = 192

        -- ✅ só valores válidos
        and active_power_kw >= 0
),

with_lag as (
    select
        *,
        lag(ts_utc) over (
            partition by power_plant_id, device_id, date_local
            order by ts_utc
        ) as prev_ts_utc
    from filtered
),

intervals as (
    select
        power_plant_id,
        power_plant_name,
        date_local as date,
        device_id,
        active_power_kw,

        extract(epoch from (ts_utc - prev_ts_utc)) as dt_seconds
    from with_lag
),

energy_rows as (
    select
        power_plant_id,
        power_plant_name,
        date,
        device_id,

        case
            when dt_seconds is null then 0
            when dt_seconds <= 0 then 0

            -- ✅ proteção contra buraco grande (ajuste se quiser)
            -- 900s = 15 min (bom pra base 5 min com tolerância)
            when dt_seconds > 900 then 0

            else active_power_kw * (dt_seconds / 3600.0)
        end as energy_kwh
    from intervals
),

per_plant_daily as (
    select
        power_plant_id,
        power_plant_name,
        date,
        sum(energy_kwh) as energy_kwh
    from energy_rows
    group by 1,2,3
)

select
    power_plant_id,
    power_plant_name,
    date,
    round(coalesce(energy_kwh, 0), 3) as energy_kwh
from per_plant_daily