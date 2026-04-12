{{ config(materialized = 'table') }}

with bounds as (

    select
        (date_trunc('day', now() at time zone 'America/Fortaleza')
            at time zone 'America/Fortaleza') as start_ts,
        ((date_trunc('day', now() at time zone 'America/Fortaleza') + interval '1 day')
            at time zone 'America/Fortaleza') as end_ts
),

active_devices as (

    select
        d.id as device_id
    from public.device d
    where d.is_active = true
),

base_analog as (

    select
        ia.power_plant_id,
        ia.device_id,
        ia.timestamp,
        ia.active_power_kw,
        date_bin(
            '5 minutes',
            ia.timestamp,
            timestamptz '2000-01-01 00:00:00+00'
        ) as ts_5min
    from {{ ref('stg_inverter_analog') }} ia
    inner join active_devices ad
        on ad.device_id = ia.device_id
    cross join bounds b
    where ia.active_power_kw is not null
      and ia.timestamp >= b.start_ts
      and ia.timestamp <  b.end_ts
),

dedup as (

    select *
    from (
        select
            b.*,
            row_number() over (
                partition by b.power_plant_id, b.device_id, b.ts_5min
                order by b.timestamp desc
            ) as rn
        from base_analog b
    ) x
    where rn = 1
),

comm_today as (

    select
        e.power_plant_id,
        e.device_id,
        e.timestamp,
        case
            when nullif(e.raw_value::text, '') ~ '^[0-9]+(\.[0-9]+)?$'
                then nullif(e.raw_value::text, '')::numeric
            when lower(e.raw_value::text) = 'true'
                then 28
            when lower(e.raw_value::text) = 'false'
                then 192
            when e.value = 1
                then 28
            when e.value = 0
                then 192
            when e.value in (28, 192)
                then e.value
            else null
        end as communication_fault_code
    from {{ ref('int_events_alarms') }} e
    inner join active_devices ad
        on ad.device_id = e.device_id
    cross join bounds b
    where e.code = 'communication_fault'
      and e.event_source = 'inverter'
      and e.timestamp >= b.start_ts
      and e.timestamp <  b.end_ts
),

comm_last_before_today as (

    select *
    from (
        select
            e.power_plant_id,
            e.device_id,
            e.timestamp,
            case
                when nullif(e.raw_value::text, '') ~ '^[0-9]+(\.[0-9]+)?$'
                    then nullif(e.raw_value::text, '')::numeric
                when lower(e.raw_value::text) = 'true'
                    then 28
                when lower(e.raw_value::text) = 'false'
                    then 192
                when e.value = 1
                    then 28
                when e.value = 0
                    then 192
                when e.value in (28, 192)
                    then e.value
                else null
            end as communication_fault_code,
            row_number() over (
                partition by e.power_plant_id, e.device_id
                order by e.timestamp desc
            ) as rn
        from {{ ref('int_events_alarms') }} e
        inner join active_devices ad
            on ad.device_id = e.device_id
        cross join bounds b
        where e.code = 'communication_fault'
          and e.event_source = 'inverter'
          and e.timestamp < b.start_ts
    ) x
    where rn = 1
),

comm_events as (

    select power_plant_id, device_id, timestamp, communication_fault_code
    from comm_today

    union all

    select power_plant_id, device_id, timestamp, communication_fault_code
    from comm_last_before_today
),

comm_intervals as (

    select
        c.power_plant_id,
        c.device_id,
        c.timestamp as status_start_ts,
        lead(c.timestamp) over (
            partition by c.power_plant_id, c.device_id
            order by c.timestamp
        ) as next_status_ts,
        c.communication_fault_code
    from comm_events c
),

dedup_with_status as (

    select
        d.power_plant_id,
        d.device_id,
        d.timestamp,
        d.ts_5min,
        d.active_power_kw,
        ci.communication_fault_code
    from dedup d
    left join comm_intervals ci
        on ci.power_plant_id = d.power_plant_id
       and ci.device_id = d.device_id
       and d.timestamp >= ci.status_start_ts
       and (
            d.timestamp < ci.next_status_ts
            or ci.next_status_ts is null
       )
),

inverter_5min as (

    select
        power_plant_id,
        ts_5min,
        sum(active_power_kw) filter (
            where communication_fault_code = 192
        ) as active_power_kw,
        count(*) filter (
            where communication_fault_code = 192
        ) as inverter_count_ok,
        count(*) filter (
            where communication_fault_code = 28
        ) as inverter_count_fault,
        count(*) filter (
            where communication_fault_code is null
        ) as inverter_count_null
    from dedup_with_status
    group by 1,2
),

weather_5min as (

    select
        ws.power_plant_id,
        date_bin(
            '5 minutes',
            ws.timestamp,
            timestamptz '2000-01-01 00:00:00+00'
        ) as ts_5min,
        avg(ws.irradiance_ghi_wm2) filter (
            where ws.communication_fault = 192
        ) as irradiance_ghi_wm2,
        avg(ws.irradiance_poa_wm2) filter (
            where ws.communication_fault = 192
        ) as irradiance_poa_wm2
    from {{ ref('stg_weather_station_analog') }} ws
    inner join active_devices ad
        on ad.device_id = ws.device_id
    cross join bounds b
    where (ws.irradiance_ghi_wm2 is not null or ws.irradiance_poa_wm2 is not null)
      and ws.timestamp >= b.start_ts
      and ws.timestamp <  b.end_ts
    group by 1,2
)

select
    i.power_plant_id,
    i.ts_5min as ts,
    coalesce(i.active_power_kw, 0) as active_power_kw,
    w.irradiance_ghi_wm2,
    w.irradiance_poa_wm2,
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