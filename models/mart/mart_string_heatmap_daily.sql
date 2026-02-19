{{ config(
    materialized = 'table'
) }}

with base as (

    select
        power_plant_id,
        power_plant_name,
        device_id       as inverter_id,
        device_name     as inverter_name,
        string_index,
        date(timestamp) as date,
        string_current_a
    from {{ ref('int_inverter_string') }}
    where string_current_a is not null
      and string_enabled = true
),

string_avg as (

    select
        power_plant_id,
        inverter_id,
        date,
        avg(string_current_a) as string_current_avg_a
    from base
    group by
        power_plant_id,
        inverter_id,
        date
),

plant_avg as (

    select
        power_plant_id,
        inverter_id,
        date,
        avg(string_current_avg_a) as inverter_string_avg_a
    from string_avg
    group by
        power_plant_id,
        inverter_id,
        date
)

select
    b.power_plant_id,
    b.power_plant_name,
    b.inverter_id,
    b.inverter_name,
    b.string_index,
    b.date,

    round(avg(b.string_current_a), 3)         as string_avg_current_a,
    round(p.inverter_string_avg_a, 3)         as inverter_avg_current_a,
    round(
        avg(b.string_current_a) - p.inverter_string_avg_a,
        3
    )                                         as deviation_from_avg_a

from base b
join plant_avg p
  on p.power_plant_id = b.power_plant_id
 and p.inverter_id    = b.inverter_id
 and p.date           = b.date

group by
    b.power_plant_id,
    b.power_plant_name,
    b.inverter_id,
    b.inverter_name,
    b.string_index,
    b.date,
    p.inverter_string_avg_a
