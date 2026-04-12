{{ config(
    materialized = 'table'
) }}

with inverter_devices as (

    select distinct
        d.id as device_id,
        coalesce(d.power_plant_id, r.power_plant_id) as power_plant_id
    from public.device d
    inner join {{ source('public', 'raw_inverter') }} r
        on r.device_id = d.id
    where d.is_active = true

),

relay_devices as (

    select distinct
        d.id as device_id,
        coalesce(d.power_plant_id, r.power_plant_id) as power_plant_id
    from public.device d
    inner join {{ source('public', 'raw_relay') }} r
        on r.device_id = d.id
    where d.is_active = true

),

inverter_latest as (

    select
        r.power_plant_id,
        r.device_id,
        nullif(trim(r.json_data ->> 'state_operation'), '')::integer as state_operation,
        nullif(trim(r.json_data ->> 'communication_fault'), '')::integer as communication_fault,
        row_number() over (
            partition by r.device_id
            order by r.timestamp desc
        ) as rn
    from {{ source('public', 'raw_inverter') }} r
    inner join inverter_devices d
        on d.device_id = r.device_id

),

relay_latest as (

    select
        r.power_plant_id,
        r.device_id,
        nullif(trim(r.json_data ->> 'status_relay'), '')::integer as status_relay,
        nullif(trim(r.json_data ->> 'communication_fault'), '')::integer as communication_fault,
        row_number() over (
            partition by r.device_id
            order by r.timestamp desc
        ) as rn
    from {{ source('public', 'raw_relay') }} r
    inner join relay_devices d
        on d.device_id = r.device_id

),

inverter_summary as (

    select
        d.power_plant_id,
        count(*) as total_inverters,
        sum(
            case
                when l.state_operation in (0, 1, 3) then 0
                when l.communication_fault = 28 then 0
                else 1
            end
        ) as ok_inverters
    from inverter_devices d
    left join inverter_latest l
        on l.device_id = d.device_id
       and l.rn = 1
    group by d.power_plant_id

),

relay_summary as (

    select
        d.power_plant_id,
        count(*) as total_relays,
        sum(
            case
                when l.communication_fault = 192
                 and l.status_relay is not null then 1
                else 0
            end
        ) as ok_relays
    from relay_devices d
    left join relay_latest l
        on l.device_id = d.device_id
       and l.rn = 1
    group by d.power_plant_id

),

plants as (

    select power_plant_id from inverter_summary
    union
    select power_plant_id from relay_summary

)

select
    p.power_plant_id,
    pp.name as power_plant_name,

    coalesce(i.total_inverters, 0) as total_inverters,
    coalesce(i.ok_inverters, 0) as ok_inverters,
    round(
        100.0 * coalesce(i.ok_inverters, 0)::numeric
        / nullif(i.total_inverters, 0),
        2
    ) as percent_inverters,

    coalesce(r.total_relays, 0) as total_relays,
    coalesce(r.ok_relays, 0) as ok_relays,
    round(
        100.0 * coalesce(r.ok_relays, 0)::numeric
        / nullif(r.total_relays, 0),
        2
    ) as percent_relays

from plants p
left join inverter_summary i
    on i.power_plant_id = p.power_plant_id
left join relay_summary r
    on r.power_plant_id = p.power_plant_id
left join public.power_plant pp
    on pp.id = p.power_plant_id

order by p.power_plant_id