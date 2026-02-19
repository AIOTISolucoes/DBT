{{ config(materialized = 'view') }}

with events as (

    select
        timestamp,
        power_plant_id,
        device_id,

        max(comm_lost::int)::boolean    as comm_lost,
        max(fault_active::int)::boolean as fault_active

    from {{ ref('int_inverter_event') }}
    group by 1,2,3
),

analog as (

    select
        timestamp,
        power_plant_id,
        device_id,
        is_generating
    from {{ ref('int_inverter_analog') }}
)

select
    coalesce(a.timestamp, e.timestamp)             as timestamp,
    coalesce(a.power_plant_id, e.power_plant_id)   as power_plant_id,
    coalesce(a.device_id, e.device_id)             as device_id,

    case
        when e.comm_lost = true then 0                     -- sem comunicação
        when e.fault_active = true then 3                  -- falha / shutdown
        when a.is_generating = true then 2                 -- rodando
        when e.comm_lost = false
             and e.fault_active = false
             and a.is_generating = false then 1            -- standby
        else 4                                              -- desconhecido
    end as inverter_status_code

from analog a
full outer join events e
  on a.device_id = e.device_id
 and a.timestamp = e.timestamp
