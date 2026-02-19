{{ config(materialized = 'view') }}

with base as (

    select
        s.timestamp,
        s.power_plant_id,
        pp.name            as power_plant_name,

        s.device_id,
        d.name             as device_name,
        dt.name            as device_type,

        s.string_index,
        s.string_current   as string_current_a

    from {{ ref('stg_inverter_string') }} s
    join device d
      on d.id = s.device_id
     and d.is_active = true
    join device_type dt
      on dt.id = d.device_type_id
    join power_plant pp
      on pp.id = s.power_plant_id
     and pp.is_active = true
),

cfg as (

    select
        customer_id,
        plant_id    as power_plant_id,
        inverter_id as device_id,
        string_index,
        enabled
    from public.inverter_string_config
)

select
    b.*,

    -- 🔑 FONTE DA VERDADE DA STRING
    coalesce(cfg.enabled, true) as string_enabled

from base b
left join cfg
  on cfg.power_plant_id = b.power_plant_id
 and cfg.device_id      = b.device_id
 and cfg.string_index   = b.string_index
