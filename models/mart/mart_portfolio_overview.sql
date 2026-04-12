{{ config(
    materialized = 'table',
    on_schema_change = 'sync_all_columns'
) }}

with params as (

    select
        (now() at time zone 'America/Fortaleza')::date as local_today,
        now() - interval '10 minutes' as freshness_cutoff_ts

),

plant_base as (

    select
        pp.id as power_plant_id,
        pp.name::text as power_plant_name,
        pp.customer_id,
        c.name::text as customer_name,
        coalesce(pp.capacity_dc, 0)::numeric as rated_power_kwp,
        coalesce(pp.capacity_ac, pp.capacity_dc, 0)::numeric as rated_power_ac_kw
    from public.power_plant pp
    left join public.customer c
        on pp.customer_id = c.id

),

customer_rollup as (

    select
        customer_id,
        sum(rated_power_kwp) as customer_rated_power_kwp
    from plant_base
    group by customer_id

),

active_devices as (

    select
        d.id as device_id,
        d.power_plant_id,
        d.name::text as device_name,
        d.device_type_id,
        dt.name::text as device_type_name
    from public.device d
    left join public.device_type dt
        on d.device_type_id = dt.id
    where coalesce(d.is_active, false) = true

),

latest_points_fresh as (

    select
        lp.*
    from {{ ref('mart_latest_reading') }} lp
    inner join active_devices ad
        on lp.device_id = ad.device_id
    cross join params p
    where lp.timestamp >= p.freshness_cutoff_ts

),

power_candidates as (

    select
        lp.power_plant_id,
        lp.device_id,
        lp.reading_source,
        lp.timestamp,
        lp.point_name,
        lp.point_value,
        case
            when lp.point_name = 'active_power_kw' then 1
            when lp.point_name = 'power_kw' then 2
            when lp.point_name = 'pac_kw' then 3
            else 99
        end as point_priority
    from latest_points_fresh lp
    where lp.reading_source in ('inverter', 'meter')
      and lp.point_name in ('active_power_kw', 'power_kw', 'pac_kw')
      and lp.point_value is not null

),

power_latest_per_device as (

    select
        power_plant_id,
        device_id,
        reading_source,
        timestamp,
        point_name,
        point_value,
        row_number() over (
            partition by power_plant_id, device_id, reading_source
            order by timestamp desc, point_priority asc
        ) as rn
    from power_candidates

),

plant_current_power as (

    select
        power_plant_id,

        sum(point_value) filter (
            where reading_source = 'inverter'
              and rn = 1
        ) as active_power_inverter_kw,

        sum(point_value) filter (
            where reading_source = 'meter'
              and rn = 1
        ) as active_power_meter_kw

    from power_latest_per_device
    group by power_plant_id

),

plant_current_irradiance as (

    select
        power_plant_id,
        coalesce(
            avg(point_value) filter (
                where reading_source = 'weather_station'
                  and point_name = 'irradiance_poa_wm2'
            ),
            avg(point_value) filter (
                where reading_source = 'weather_station'
                  and point_name = 'irradiance_ghi_wm2'
            ),
            avg(point_value) filter (
                where reading_source = 'weather_station'
                  and point_name in (
                      'irradiance_wm2',
                      'poa_irradiance_wm2',
                      'irradiance',
                      'irr_global_wm2',
                      'ghi_wm2'
                  )
            )
        ) as irradiance_wm2
    from latest_points_fresh
    group by power_plant_id

),

device_inventory as (

    select
        ad.power_plant_id,
        count(*) filter (
            where lower(coalesce(ad.device_type_name, '')) like '%inverter%'
        ) as inverter_total,
        count(*) filter (
            where lower(coalesce(ad.device_type_name, '')) like '%relay%'
        ) as relay_total
    from active_devices ad
    group by ad.power_plant_id

),

latest_alarm_state as (

    select distinct on (
        e.power_plant_id,
        e.device_id,
        e.code
    )
        e.power_plant_id,
        e.device_id,
        e.device_type_name,
        e.code,
        e.value,
        e.severity,
        e.is_active_event,
        e.timestamp
    from {{ ref('int_events_alarms') }} e
    inner join active_devices ad
        on e.device_id = ad.device_id
    order by
        e.power_plant_id,
        e.device_id,
        e.code,
        e.timestamp desc

),

active_alarms as (

    select *
    from latest_alarm_state
    where is_active_event = true

),

device_unavailability as (

    select
        power_plant_id,

        count(distinct device_id) filter (
            where lower(coalesce(device_type_name, '')) like '%inverter%'
              and (
                  code = 'communication_fault'
                  or lower(coalesce(severity, '')) in ('critical', 'high')
              )
        ) as inverter_unavailable,

        count(distinct device_id) filter (
            where lower(coalesce(device_type_name, '')) like '%relay%'
              and (
                  code = 'communication_fault'
                  or lower(coalesce(severity, '')) in ('critical', 'high')
              )
        ) as relay_unavailable

    from active_alarms
    group by power_plant_id

),

plant_alarm_rollup as (

    select
        power_plant_id,

        count(*) as active_alarm_count,

        count(*) filter (
            where lower(coalesce(severity, '')) in ('critical', 'high')
        ) as red_alarm_count,

        count(*) filter (
            where lower(coalesce(severity, '')) = 'warning'
        ) as yellow_alarm_count,

        case
            when count(*) filter (
                where lower(coalesce(severity, '')) in ('critical', 'high')
            ) > 0 then 'red'
            when count(*) filter (
                where lower(coalesce(severity, '')) = 'warning'
            ) > 0 then 'yellow'
            else 'green'
        end as plant_status_color

    from active_alarms
    group by power_plant_id

),

plant_perf_today as (

    select
        f.power_plant_id,
        f.generation_daily_kwh,
        f.generation_liquid_meter_kwh,
        f.irradiation_daily_kwh_m2,
        f.pr_daily_pct,
        f.capacity_factor_daily_pct
    from {{ ref('fct_power_plant_metrics_daily') }} f
    cross join params p
    where f.date_day = p.local_today

),

plant_perf_latest as (

    select distinct on (power_plant_id)
        power_plant_id,
        date_day,
        generation_accumulated_kwh,
        irradiation_accumulated_kwh_m2,
        pr_accumulated_pct,
        capacity_factor_accumulated_pct
    from {{ ref('fct_power_plant_metrics_daily') }}
    order by power_plant_id, date_day desc

),

final as (

    select
        pb.customer_id,
        pb.customer_name,
        cr.customer_rated_power_kwp,

        pb.power_plant_id,
        pb.power_plant_name,

        pb.rated_power_kwp,
        pb.rated_power_ac_kw,

        coalesce(pcp.active_power_inverter_kw, 0) as active_power_inverter_kw,
        coalesce(pcp.active_power_meter_kw, 0) as active_power_meter_kw,
        coalesce(pcp.active_power_meter_kw, pcp.active_power_inverter_kw, 0) as active_power_kw,

        coalesce(ppt.generation_daily_kwh, 0) as daily_energy_kwh,
        coalesce(ppt.generation_liquid_meter_kwh, 0) as generation_liquid_meter_kwh,
        coalesce(ppl.generation_accumulated_kwh, 0) as generation_accumulated_kwh,

        pci.irradiance_wm2,
        ppl.irradiation_accumulated_kwh_m2,

        ppt.pr_daily_pct,
        ppl.pr_accumulated_pct,

        ppt.capacity_factor_daily_pct,
        ppl.capacity_factor_accumulated_pct as capacity_factor_pct,

        case
            when coalesce(di.inverter_total, 0) > 0
            then round(
                100.0 * (
                    di.inverter_total - coalesce(du.inverter_unavailable, 0)
                ) / di.inverter_total,
                2
            )
            else null
        end as inverter_availability_pct,

        case
            when coalesce(di.relay_total, 0) > 0
            then round(
                100.0 * (
                    di.relay_total - coalesce(du.relay_unavailable, 0)
                ) / di.relay_total,
                2
            )
            else null
        end as relay_availability_pct,

        coalesce(par.plant_status_color, 'green') as plant_status_color,
        coalesce(par.red_alarm_count, 0) as red_alarm_count,
        coalesce(par.yellow_alarm_count, 0) as yellow_alarm_count,
        coalesce(par.active_alarm_count, 0) as active_alarm_count,

        now() as updated_at

    from plant_base pb
    left join customer_rollup cr
        on pb.customer_id = cr.customer_id
    left join plant_current_power pcp
        on pb.power_plant_id = pcp.power_plant_id
    left join plant_current_irradiance pci
        on pb.power_plant_id = pci.power_plant_id
    left join plant_perf_today ppt
        on pb.power_plant_id = ppt.power_plant_id
    left join plant_perf_latest ppl
        on pb.power_plant_id = ppl.power_plant_id
    left join device_inventory di
        on pb.power_plant_id = di.power_plant_id
    left join device_unavailability du
        on pb.power_plant_id = du.power_plant_id
    left join plant_alarm_rollup par
        on pb.power_plant_id = par.power_plant_id

)

select *
from final
