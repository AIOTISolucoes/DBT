{{ config(
    materialized = 'table',
    on_schema_change = 'sync_all_columns'
) }}

with energy_today as (

    -- ✅ último dia POR USINA
    select
        power_plant_id,
        energy_kwh
    from (
        select
            power_plant_id,
            date,
            energy_kwh,
            row_number() over (
                partition by power_plant_id
                order by date desc
            ) as rn
        from {{ ref('mart_energy_daily') }}
    ) t
    where rn = 1
),

pr_today as (

    -- ✅ último dia POR USINA
    select
        power_plant_id,
        performance_ratio
    from (
        select
            power_plant_id,
            date,
            performance_ratio,
            row_number() over (
                partition by power_plant_id
                order by date desc
            ) as rn
        from {{ ref('mart_performance_ratio_daily') }}
    ) t
    where rn = 1
),

alarms_today as (

    -- ✅ último dia POR USINA (com agregação por dia)
    select
        power_plant_id,
        critical_alarms
    from (
        select
            power_plant_id,
            date,
            sum(critical_alarms) as critical_alarms,
            row_number() over (
                partition by power_plant_id
                order by date desc
            ) as rn
        from {{ ref('mart_alarm_summary_daily') }}
        group by power_plant_id, date
    ) t
    where rn = 1
),

-- ✅ DISPONIBILIDADE DOS INVERSORES (último dia por usina)
availability_inverter as (

    select
        power_plant_id,
        inverter_availability_pct
    from (
        select
            power_plant_id,
            date,
            avg(availability_pct) as inverter_availability_pct,
            row_number() over (
                partition by power_plant_id
                order by date desc
            ) as rn
        from {{ ref('mart_device_availability_daily') }}
        where device_type = 'inverter'
        group by power_plant_id, date
    ) t
    where rn = 1
),

-- ✅ DISPONIBILIDADE DOS RELAYS (último dia por usina)
availability_relay as (

    select
        power_plant_id,
        relay_availability_pct
    from (
        select
            power_plant_id,
            date,
            avg(availability_pct) as relay_availability_pct,
            row_number() over (
                partition by power_plant_id
                order by date desc
            ) as rn
        from {{ ref('mart_device_availability_daily') }}
        where device_type = 'relay'
        group by power_plant_id, date
    ) t
    where rn = 1
),

-- ✅ pega o último registro de cada INVERSOR ATIVO (filtra impostores)
latest_inverter_analog as (

    select
        a.power_plant_id,
        a.device_id,
        a.active_power_kw,
        a.timestamp,
        row_number() over (
            partition by a.power_plant_id, a.device_id
            order by a.timestamp desc
        ) as rn
    from {{ ref('int_inverter_analog') }} a

    join {{ source('public','device') }} d
      on d.id = a.device_id
     and d.is_active = true

    join {{ source('public','device_type') }} dt
      on dt.id = d.device_type_id
     and dt.name = 'inverter'
),

-- ✅ FILTRO DE "FRESCOR" (não soma device parado/antigo)
-- ajuste o intervalo conforme frequência real do CLP (ex.: 5/10/15 min)
active_power_now as (

    select
        power_plant_id,
        sum(active_power_kw) as active_power_kw,
        max(timestamp)       as last_update
    from latest_inverter_analog
    where rn = 1
      and timestamp >= now() - interval '15 minutes'
    group by power_plant_id
),

-- ✅ CONTADORES DE INVERSORES (GEN / NO COMM / OFF) POR USINA
inverter_counts as (

    select
        power_plant_id,

        count(*) as inverter_total,

        sum(case when inverter_status_code = 2 then 1 else 0 end) as inverter_generating, -- Gen (RUNNING)
        sum(case when inverter_status_code = 0 then 1 else 0 end) as inverter_no_comm,    -- No comm (OFFLINE)
        sum(case when inverter_status_code in (1,3) then 1 else 0 end) as inverter_off    -- Off (STANDBY ou FAULT)

    from {{ ref('mart_inverter_realtime') }}
    group by 1
)

select
    -- 🔐 MULTI-CLIENTE (TENANT)
    p.customer_id,

    -- 🔹 IDENTIDADE DA USINA
    p.id   as power_plant_id,
    p.name as power_plant_name,

    -- 🔹 POTÊNCIA INSTALADA (AC)
    p.capacity_ac as rated_power_kw,

    ap.last_update,

    -- 🔹 PRODUÇÃO
    coalesce(ap.active_power_kw, 0)  as active_power_kw,
    coalesce(e.energy_kwh, 0)        as energy_today_kwh,

    -- 🔹 PERFORMANCE
    pr.performance_ratio,

    -- 🔹 CLIMA
    w.irradiance_ghi_wm2,
    w.irradiance_poa_wm2,
    w.air_temperature_c,
    w.module_temperature_c,

    -- 🔹 DISPONIBILIDADE
    coalesce(ai.inverter_availability_pct, 1) as inverter_availability_pct,
    ar.relay_availability_pct                 as relay_availability_pct,

    -- 🔹 CONTADORES INVERSORES (chips do header)
    coalesce(ic.inverter_total, 0)       as inverter_total,
    coalesce(ic.inverter_generating, 0)  as inverter_generating,
    coalesce(ic.inverter_no_comm, 0)     as inverter_no_comm,
    coalesce(ic.inverter_off, 0)         as inverter_off,

    -- 🔹 ALARMES
    coalesce(al.critical_alarms, 0)      as critical_alarms,

    -- 🔹 STATUS GERAL
    case
        when coalesce(al.critical_alarms, 0) > 0
            then 'ALERT'
        when coalesce(ai.inverter_availability_pct, 1) < 0.95
            then 'WARNING'
        else 'OK'
    end as plant_status

from {{ source('public', 'power_plant') }} p

left join active_power_now ap
  on ap.power_plant_id = p.id

left join energy_today e
  on e.power_plant_id = p.id

left join pr_today pr
  on pr.power_plant_id = p.id

left join {{ ref('mart_weather_realtime') }} w
  on w.power_plant_id = p.id

left join alarms_today al
  on al.power_plant_id = p.id

left join availability_inverter ai
  on ai.power_plant_id = p.id

left join availability_relay ar
  on ar.power_plant_id = p.id

left join inverter_counts ic
  on ic.power_plant_id = p.id

where p.is_active = true
