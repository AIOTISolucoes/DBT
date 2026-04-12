{{ config(
    materialized = 'incremental',
    unique_key = ['power_plant_id', 'date_day'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        a.power_plant_id,
        a.device_id,
        a.timestamp,
        a.timestamp::date as date_day,
        a.daily_active_energy_kwh,
        a.cumulative_active_energy_kwh,
        a.working_status,
        a.state_operation
    from {{ ref('stg_inverter_analog') }} a
    where a.power_plant_id is not null
    {% if is_incremental() %}
      and a.timestamp::date >= current_date - 1
    {% endif %}

),

device_edges as (

    select distinct
        power_plant_id,
        device_id,
        date_day,

        first_value(timestamp) over (
            partition by power_plant_id, device_id, date_day
            order by timestamp desc
        ) as last_timestamp,

        first_value(daily_active_energy_kwh) over (
            partition by power_plant_id, device_id, date_day
            order by
                case when daily_active_energy_kwh is not null then 0 else 1 end,
                timestamp desc
        ) as last_daily_energy,

        first_value(cumulative_active_energy_kwh) over (
            partition by power_plant_id, device_id, date_day
            order by
                case
                    when cumulative_active_energy_kwh is not null
                     and cumulative_active_energy_kwh > 0 then 0
                    else 1
                end,
                timestamp asc
        ) as first_cum_energy_valid,

        first_value(cumulative_active_energy_kwh) over (
            partition by power_plant_id, device_id, date_day
            order by
                case
                    when cumulative_active_energy_kwh is not null
                     and cumulative_active_energy_kwh > 0 then 0
                    else 1
                end,
                timestamp desc
        ) as last_cum_energy_valid,

        first_value(working_status) over (
            partition by power_plant_id, device_id, date_day
            order by timestamp desc
        ) as last_working_status,

        first_value(state_operation) over (
            partition by power_plant_id, device_id, date_day
            order by timestamp desc
        ) as last_state_operation

    from base

),

device_daily as (

    select distinct
        power_plant_id,
        device_id,
        date_day,
        last_timestamp,

        case
            when coalesce(last_daily_energy, 0) > 0
                then last_daily_energy

            when first_cum_energy_valid is not null
             and last_cum_energy_valid is not null
             and first_cum_energy_valid > 0
             and last_cum_energy_valid >= first_cum_energy_valid
                then last_cum_energy_valid - first_cum_energy_valid

            else 0
        end as generation_device_kwh,

        last_working_status,
        last_state_operation

    from device_edges

),

plant_ref as (

    select
        power_plant_id,
        date_day,
        max(last_timestamp) as plant_last_timestamp
    from device_daily
    group by 1,2

),

device_daily_fresh as (

    select d.*
    from device_daily d
    join plant_ref p
      on p.power_plant_id = d.power_plant_id
     and p.date_day = d.date_day
    where d.last_timestamp >= p.plant_last_timestamp - interval '20 minutes'
      and (
            coalesce(d.last_working_status, 0) <> 0
         or coalesce(d.last_state_operation, 0) <> 0
      )
),

plant_daily as (

    select
        power_plant_id,
        date_day,
        sum(generation_device_kwh) as generation_daily_kwh,
        max(last_timestamp) as last_timestamp
    from device_daily_fresh
    group by 1,2

),

final as (

    select
        md5(
            coalesce(power_plant_id::text, '') || '|' ||
            coalesce(date_day::text, '')
        ) as power_plant_daily_row_id,
        power_plant_id,
        date_day,
        generation_daily_kwh,
        last_timestamp,
        now() as updated_at
    from plant_daily

)

select *
from final