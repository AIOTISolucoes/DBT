{{ config(
    materialized = 'table',
    on_schema_change = 'sync_all_columns'
) }}

with plant_base as (

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

pr_window as (

    select
        time '04:10:00' as start_time,
        time '20:00:00' as end_time

),

source_rows as (

    select
        'inverter'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_inverter_analog') }} a

    union all

    select
        'meter'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_meter_analog') }} a

    union all

    select
        'weather_station'::text as reading_source,
        a.timestamp,
        a.power_plant_id,
        a.device_id,
        to_jsonb(a)
            - 'id'
            - 'timestamp'
            - 'power_plant_id'
            - 'device_id'
            - 'created_at'
            - 'updated_at' as analog_json
    from {{ ref('stg_weather_station_analog') }} a

),

all_analog_points as (

    select
        s.reading_source,
        s.timestamp,
        s.power_plant_id,
        s.device_id,
        kv.key as point_name,
        case
            when jsonb_typeof(kv.value) = 'number'
                then (kv.value #>> '{}')::numeric
            when jsonb_typeof(kv.value) = 'string'
                 and btrim(kv.value #>> '{}') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
                then btrim(kv.value #>> '{}')::numeric
            else null
        end as point_value
    from source_rows s
    cross join lateral jsonb_each(s.analog_json) as kv(key, value)
    where
        jsonb_typeof(kv.value) = 'number'
        or (
            jsonb_typeof(kv.value) = 'string'
            and btrim(kv.value #>> '{}') ~ '^[+-]?((\d+(\.\d+)?)|(\.\d+))$'
        )

),

available_days as (

    select distinct
        power_plant_id,
        date_trunc('day', timestamp)::date as date_day
    from all_analog_points

),

generation_counter_candidates as (

    select
        ap.power_plant_id,
        ap.device_id,
        date_trunc('day', ap.timestamp)::date as date_day,
        ap.point_name,
        case
            when ap.point_name = 'daily_active_energy_kwh' then 1
            when ap.point_name = 'daily_energy_kwh' then 2
            when ap.point_name = 'energy_today_kwh' then 3
            when ap.point_name = 'generation_today_kwh' then 4
            when ap.point_name = 'cumulative_active_energy_kwh' then 5
            when ap.point_name = 'total_energy_kwh' then 6
            when ap.point_name = 'energy_total_kwh' then 7
            when ap.point_name = 'yield_total_kwh' then 8
            else 99
        end as point_priority,
        ap.timestamp,
        ap.point_value
    from all_analog_points ap
    where ap.reading_source = 'inverter'
      and ap.point_name in (
          'daily_active_energy_kwh',
          'daily_energy_kwh',
          'energy_today_kwh',
          'generation_today_kwh',
          'cumulative_active_energy_kwh',
          'total_energy_kwh',
          'energy_total_kwh',
          'yield_total_kwh'
      )

),

generation_counter_stats as (

    select
        power_plant_id,
        device_id,
        date_day,
        point_name,
        point_priority,
        min(point_value) as min_value,
        max(point_value) as max_value
    from generation_counter_candidates
    where point_value is not null
    group by 1, 2, 3, 4, 5

),

generation_counter_device_ranked as (

    select
        power_plant_id,
        device_id,
        date_day,
        point_name,
        point_priority,
        case
            when point_name in (
                'daily_active_energy_kwh',
                'daily_energy_kwh',
                'energy_today_kwh',
                'generation_today_kwh'
            )
                then max_value

            when point_name in (
                'cumulative_active_energy_kwh',
                'total_energy_kwh',
                'energy_total_kwh',
                'yield_total_kwh'
            )
             and max_value >= min_value
                then max_value - min_value

            else null
        end as generation_device_kwh_counter,
        row_number() over (
            partition by power_plant_id, device_id, date_day
            order by point_priority
        ) as rn
    from generation_counter_stats

),

generation_counter_daily as (

    select
        power_plant_id,
        date_day,
        sum(generation_device_kwh_counter) as generation_daily_kwh_counter
    from generation_counter_device_ranked
    where rn = 1
    group by 1, 2

),

meter_liquid_candidates as (

    select
        ap.power_plant_id,
        ap.device_id,
        date_trunc('day', ap.timestamp)::date as date_day,
        ap.point_name,
        ap.timestamp,
        ap.point_value
    from all_analog_points ap
    where ap.reading_source = 'meter'
      and ap.point_name in (
          'energy_export_kwh',
          'energy_import_kwh'
      )

),

meter_liquid_edges as (

    select distinct
        power_plant_id,
        device_id,
        date_day,
        point_name,
        first_value(point_value) over (
            partition by power_plant_id, device_id, date_day, point_name
            order by timestamp asc
        ) as start_value,
        first_value(point_value) over (
            partition by power_plant_id, device_id, date_day, point_name
            order by timestamp desc
        ) as end_value
    from meter_liquid_candidates

),

meter_liquid_device_daily as (

    select
        power_plant_id,
        device_id,
        date_day,
        max(
            case
                when point_name = 'energy_export_kwh'
                 and end_value >= start_value
                then end_value - start_value
            end
        ) as energy_export_daily_kwh,
        max(
            case
                when point_name = 'energy_import_kwh'
                 and end_value >= start_value
                then end_value - start_value
            end
        ) as energy_import_daily_kwh
    from meter_liquid_edges
    where end_value is not null
      and start_value is not null
    group by 1, 2, 3

),

meter_liquid_daily as (

    select
        power_plant_id,
        date_day,
        sum(coalesce(energy_export_daily_kwh, 0)) as energy_export_daily_kwh,
        sum(coalesce(energy_import_daily_kwh, 0)) as energy_import_daily_kwh,
        sum(coalesce(energy_export_daily_kwh, 0) - coalesce(energy_import_daily_kwh, 0)) as generation_liquid_meter_kwh
    from meter_liquid_device_daily
    group by 1, 2

),

plant_power_candidates as (

    select
        ap.power_plant_id,
        ap.device_id,
        date_trunc('day', ap.timestamp)::date as date_day,
        ap.timestamp,
        ap.point_name,
        ap.point_value,
        case
            when ap.point_name = 'active_power_kw' then 1
            when ap.point_name = 'power_kw' then 2
            when ap.point_name = 'pac_kw' then 3
            else 99
        end as point_priority
    from all_analog_points ap
    where ap.reading_source = 'inverter'
      and ap.point_name in ('active_power_kw', 'power_kw', 'pac_kw')

),

plant_power_device_ranked as (

    select
        power_plant_id,
        device_id,
        date_day,
        timestamp,
        point_name,
        point_value,
        row_number() over (
            partition by power_plant_id, device_id, timestamp
            order by point_priority
        ) as rn
    from plant_power_candidates
    where point_value is not null

),

plant_power_timeseries as (

    select
        power_plant_id,
        date_day,
        timestamp,
        sum(point_value) as plant_active_power_kw
    from plant_power_device_ranked
    where rn = 1
    group by 1, 2, 3

),

plant_power_ranked as (

    select
        *,
        lag(timestamp) over (
            partition by power_plant_id, date_day
            order by timestamp
        ) as prev_timestamp,
        lag(plant_active_power_kw) over (
            partition by power_plant_id, date_day
            order by timestamp
        ) as prev_power_kw
    from plant_power_timeseries

),

generation_power_daily as (

    select
        power_plant_id,
        date_day,
        sum(
            case
                when prev_timestamp is null then 0
                when extract(epoch from (timestamp - prev_timestamp)) / 3600.0 > 0.25 then 0
                else (
                    (coalesce(prev_power_kw, 0) + coalesce(plant_active_power_kw, 0)) / 2.0
                ) * (extract(epoch from (timestamp - prev_timestamp)) / 3600.0)
            end
        ) as generation_daily_kwh_power
    from plant_power_ranked
    group by 1, 2

),

generation_power_daily_pr as (

    select
        ppr.power_plant_id,
        ppr.date_day,
        sum(
            case
                when ppr.prev_timestamp is null then 0
                when extract(epoch from (ppr.timestamp - ppr.prev_timestamp)) / 3600.0 > 0.25 then 0
                when ppr.timestamp::time < pw.start_time then 0
                when ppr.timestamp::time > pw.end_time then 0
                else (
                    (coalesce(ppr.prev_power_kw, 0) + coalesce(ppr.plant_active_power_kw, 0)) / 2.0
                ) * (extract(epoch from (ppr.timestamp - ppr.prev_timestamp)) / 3600.0)
            end
        ) as generation_pr_window_kwh
    from plant_power_ranked ppr
    cross join pr_window pw
    group by 1, 2

),

irradiation_counter_candidates as (

    select
        ap.power_plant_id,
        date_trunc('day', ap.timestamp)::date as date_day,
        ap.point_name,
        case
            when ap.point_name = 'poa_irradiation_daily_kwh_m2' then 1
            when ap.point_name = 'irradiation_daily_kwh_m2' then 2
            when ap.point_name = 'daily_irradiation_kwh_m2' then 3
            when ap.point_name = 'irradiation_accumulated_kwh_m2' then 4
            else 99
        end as point_priority,
        ap.timestamp,
        ap.point_value
    from all_analog_points ap
    where ap.reading_source = 'weather_station'
      and ap.point_name in (
          'poa_irradiation_daily_kwh_m2',
          'irradiation_daily_kwh_m2',
          'daily_irradiation_kwh_m2',
          'irradiation_accumulated_kwh_m2'
      )

),

irradiation_counter_stats as (

    select
        power_plant_id,
        date_day,
        point_name,
        point_priority,
        min(point_value) as min_value,
        max(point_value) as max_value
    from irradiation_counter_candidates
    where point_value is not null
    group by 1, 2, 3, 4

),

irradiation_counter_daily_ranked as (

    select
        power_plant_id,
        date_day,
        point_name,
        case
            when point_name in (
                'poa_irradiation_daily_kwh_m2',
                'irradiation_daily_kwh_m2',
                'daily_irradiation_kwh_m2'
            )
                then max_value

            when point_name = 'irradiation_accumulated_kwh_m2'
             and max_value >= min_value
                then max_value - min_value

            else null
        end as irradiation_daily_kwh_m2_counter,
        row_number() over (
            partition by power_plant_id, date_day
            order by point_priority
        ) as rn
    from irradiation_counter_stats

),

irradiation_counter_daily as (

    select
        power_plant_id,
        date_day,
        irradiation_daily_kwh_m2_counter
    from irradiation_counter_daily_ranked
    where rn = 1

),

weather_irradiance_points as (

    select
        ap.power_plant_id,
        date_trunc('day', ap.timestamp)::date as date_day,
        ap.timestamp,

        max(case when ap.point_name = 'irradiance_poa_wm2' then ap.point_value end) as poa_wm2,
        max(case when ap.point_name = 'poa_irradiance_wm2' then ap.point_value end) as poa_wm2_legacy,

        max(case when ap.point_name = 'irradiance_ghi_wm2' then ap.point_value end) as ghi_wm2,
        max(case when ap.point_name = 'ghi_wm2' then ap.point_value end) as ghi_wm2_legacy,
        max(case when ap.point_name = 'irradiance_wm2' then ap.point_value end) as irradiance_generic_wm2,
        max(case when ap.point_name = 'irr_global_wm2' then ap.point_value end) as irr_global_wm2,
        max(case when ap.point_name = 'irradiance' then ap.point_value end) as irradiance_legacy

    from all_analog_points ap
    where ap.reading_source = 'weather_station'
      and ap.point_name in (
          'irradiance_poa_wm2',
          'poa_irradiance_wm2',
          'irradiance_ghi_wm2',
          'ghi_wm2',
          'irradiance_wm2',
          'irr_global_wm2',
          'irradiance'
      )
    group by 1, 2, 3

),

plant_irradiance_timeseries as (

    select
        power_plant_id,
        date_day,
        timestamp,
        coalesce(
            poa_wm2,
            poa_wm2_legacy,
            ghi_wm2,
            ghi_wm2_legacy,
            irradiance_generic_wm2,
            irr_global_wm2,
            irradiance_legacy
        ) as irradiance_wm2
    from weather_irradiance_points
    where coalesce(
        poa_wm2,
        poa_wm2_legacy,
        ghi_wm2,
        ghi_wm2_legacy,
        irradiance_generic_wm2,
        irr_global_wm2,
        irradiance_legacy
    ) is not null

),

plant_irradiance_ranked as (

    select
        *,
        lag(timestamp) over (
            partition by power_plant_id, date_day
            order by timestamp
        ) as prev_timestamp,
        lag(irradiance_wm2) over (
            partition by power_plant_id, date_day
            order by timestamp
        ) as prev_irradiance_wm2
    from plant_irradiance_timeseries

),

irradiation_integrated_daily as (

    select
        power_plant_id,
        date_day,
        sum(
            case
                when prev_timestamp is null then 0
                when extract(epoch from (timestamp - prev_timestamp)) / 3600.0 > 0.25 then 0
                else (
                    ((coalesce(prev_irradiance_wm2, 0) + coalesce(irradiance_wm2, 0)) / 2.0)
                    * (extract(epoch from (timestamp - prev_timestamp)) / 3600.0)
                ) / 1000.0
            end
        ) as irradiation_daily_kwh_m2_integrated
    from plant_irradiance_ranked
    group by 1, 2

),

irradiation_integrated_daily_pr as (

    select
        pir.power_plant_id,
        pir.date_day,
        sum(
            case
                when pir.prev_timestamp is null then 0
                when extract(epoch from (pir.timestamp - pir.prev_timestamp)) / 3600.0 > 0.25 then 0
                when pir.timestamp::time < pw.start_time then 0
                when pir.timestamp::time > pw.end_time then 0
                else (
                    ((coalesce(pir.prev_irradiance_wm2, 0) + coalesce(pir.irradiance_wm2, 0)) / 2.0)
                    * (extract(epoch from (pir.timestamp - pir.prev_timestamp)) / 3600.0)
                ) / 1000.0
            end
        ) as irradiation_pr_window_kwh_m2
    from plant_irradiance_ranked pir
    cross join pr_window pw
    group by 1, 2

),

daily_base as (

    select
        pb.customer_id,
        pb.customer_name,
        pb.power_plant_id,
        pb.power_plant_name,
        pb.rated_power_kwp,
        pb.rated_power_ac_kw,
        d.date_day,

        coalesce(
            gcd.generation_daily_kwh_counter,
            gpd.generation_daily_kwh_power,
            0
        ) as generation_daily_kwh,

        coalesce(mld.generation_liquid_meter_kwh, 0) as generation_liquid_meter_kwh,

        coalesce(
            icd.irradiation_daily_kwh_m2_counter,
            iid.irradiation_daily_kwh_m2_integrated,
            0
        ) as irradiation_daily_kwh_m2,

        coalesce(gpdpr.generation_pr_window_kwh, 0) as generation_pr_window_kwh,
        coalesce(iidpr.irradiation_pr_window_kwh_m2, 0) as irradiation_pr_window_kwh_m2

    from plant_base pb
    inner join available_days d
        on pb.power_plant_id = d.power_plant_id
    left join generation_counter_daily gcd
        on pb.power_plant_id = gcd.power_plant_id
       and d.date_day = gcd.date_day
    left join generation_power_daily gpd
        on pb.power_plant_id = gpd.power_plant_id
       and d.date_day = gpd.date_day
    left join meter_liquid_daily mld
        on pb.power_plant_id = mld.power_plant_id
       and d.date_day = mld.date_day
    left join irradiation_counter_daily icd
        on pb.power_plant_id = icd.power_plant_id
       and d.date_day = icd.date_day
    left join irradiation_integrated_daily iid
        on pb.power_plant_id = iid.power_plant_id
       and d.date_day = iid.date_day
    left join generation_power_daily_pr gpdpr
        on pb.power_plant_id = gpdpr.power_plant_id
       and d.date_day = gpdpr.date_day
    left join irradiation_integrated_daily_pr iidpr
        on pb.power_plant_id = iidpr.power_plant_id
       and d.date_day = iidpr.date_day

),

with_kpis as (

    select
        *,
        case
            when rated_power_kwp > 0
             and irradiation_pr_window_kwh_m2 > 0
            then round(
                greatest(
                    0,
                    least(
                        100,
                        100.0 * generation_pr_window_kwh
                        / (rated_power_kwp * irradiation_pr_window_kwh_m2)
                    )
                ),
                2
            )
            else null
        end as pr_daily_pct,

        case
            when rated_power_ac_kw > 0
            then round(
                greatest(
                    0,
                    least(
                        100,
                        100.0 * generation_daily_kwh
                        / (rated_power_ac_kw * 24.0)
                    )
                ),
                2
            )
            else null
        end as capacity_factor_daily_pct
    from daily_base

),

with_accum as (

    select
        *,
        sum(generation_daily_kwh) over (
            partition by power_plant_id
            order by date_day
            rows between unbounded preceding and current row
        ) as generation_accumulated_kwh,

        sum(irradiation_daily_kwh_m2) over (
            partition by power_plant_id
            order by date_day
            rows between unbounded preceding and current row
        ) as irradiation_accumulated_kwh_m2,

        sum(generation_pr_window_kwh) over (
            partition by power_plant_id
            order by date_day
            rows between unbounded preceding and current row
        ) as generation_pr_window_accumulated_kwh,

        sum(irradiation_pr_window_kwh_m2) over (
            partition by power_plant_id
            order by date_day
            rows between unbounded preceding and current row
        ) as irradiation_pr_window_accumulated_kwh_m2,

        count(*) over (
            partition by power_plant_id
            order by date_day
            rows between unbounded preceding and current row
        ) as accumulated_day_count
    from with_kpis

),

final as (

    select
        md5(
            coalesce(power_plant_id::text, '') || '|' ||
            coalesce(date_day::text, '')
        ) as power_plant_metrics_daily_row_id,

        customer_id,
        customer_name,
        power_plant_id,
        power_plant_name,
        date_day,

        rated_power_kwp,
        rated_power_ac_kw,

        generation_daily_kwh,
        generation_liquid_meter_kwh,
        irradiation_daily_kwh_m2,

        pr_daily_pct,
        capacity_factor_daily_pct,

        generation_accumulated_kwh,
        irradiation_accumulated_kwh_m2,

        case
            when rated_power_kwp > 0
             and irradiation_pr_window_accumulated_kwh_m2 > 0
            then round(
                greatest(
                    0,
                    least(
                        100,
                        100.0 * generation_pr_window_accumulated_kwh
                        / (rated_power_kwp * irradiation_pr_window_accumulated_kwh_m2)
                    )
                ),
                2
            )
            else null
        end as pr_accumulated_pct,

        case
            when rated_power_ac_kw > 0
             and accumulated_day_count > 0
            then round(
                greatest(
                    0,
                    least(
                        100,
                        100.0 * generation_accumulated_kwh
                        / (rated_power_ac_kw * accumulated_day_count * 24.0)
                    )
                ),
                2
            )
            else null
        end as capacity_factor_accumulated_pct,

        now() as updated_at

    from with_accum

)

select *
from final