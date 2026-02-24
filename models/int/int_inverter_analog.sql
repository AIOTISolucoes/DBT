{{ config(materialized = 'view') }}

with analog as (
  select *
  from {{ ref('stg_inverter_analog') }}
),

status as (
  select
    s.timestamp,
    s.power_plant_id,
    s.device_id,

    -- ✅ working_status (aceita "16" ou true/false)
    case
      when lower(coalesce(s.discrete_data_json ->> 'working_status','')) in ('true','false')
        then (lower(s.discrete_data_json ->> 'working_status') = 'true')::int
      when coalesce(s.discrete_data_json ->> 'working_status','') ~ '^[0-9]+$'
        then (s.discrete_data_json ->> 'working_status')::int
      else null
    end as working_status,

    -- ✅ inverter_status (aceita "16" ou true/false)
    case
      when lower(coalesce(s.discrete_data_json ->> 'inverter_status','')) in ('true','false')
        then (lower(s.discrete_data_json ->> 'inverter_status') = 'true')::int
      when coalesce(s.discrete_data_json ->> 'inverter_status','') ~ '^[0-9]+$'
        then (s.discrete_data_json ->> 'inverter_status')::int
      else null
    end as inverter_status,

    -- ✅ communication_fault_code (valor cru: 28/192/0/1 ou boolean)
    case
      when lower(coalesce(s.discrete_data_json ->> 'communication_fault','')) in ('true','false')
        then (lower(s.discrete_data_json ->> 'communication_fault') = 'true')::int
      when coalesce(s.discrete_data_json ->> 'communication_fault','') ~ '^[0-9]+$'
        then (s.discrete_data_json ->> 'communication_fault')::int
      else null
    end as communication_fault_code,

    -- ✅ is_communication_ok (normalizado 1=ok / 0=falha)
    case
      when lower(coalesce(s.discrete_data_json ->> 'communication_fault','')) in ('true','false') then
        -- true = falha? ou true = ok? aqui estamos tratando true como "falha" (ajuste se precisar)
        case when lower(s.discrete_data_json ->> 'communication_fault') = 'true' then 0 else 1 end

      when coalesce(s.discrete_data_json ->> 'communication_fault','') ~ '^[0-9]+$' then
        case
          -- regra SCADA do projeto
          when (s.discrete_data_json ->> 'communication_fault')::int = 28  then 0  -- falha
          when (s.discrete_data_json ->> 'communication_fault')::int = 192 then 1  -- ok

          -- fallback PLC comum: 0=ok, 1=falha
          when (s.discrete_data_json ->> 'communication_fault')::int = 0 then 1
          when (s.discrete_data_json ->> 'communication_fault')::int = 1 then 0

          else null
        end

      else null
    end as is_communication_ok,

    row_number() over (
      partition by s.timestamp, s.power_plant_id, s.device_id
      order by s.timestamp desc
    ) as rn
  from {{ ref('stg_inverter_event') }} s
),

status_dedup as (
  select
    timestamp,
    power_plant_id,
    device_id,
    working_status,
    inverter_status,
    communication_fault_code,
    is_communication_ok
  from status
  where rn = 1
)

select
    a.timestamp,
    a.power_plant_id,
    pp.name               as power_plant_name,
    pp.capacity_ac,
    pp.capacity_dc,

    a.device_id,
    d.name                as device_name,
    dt.name               as device_type,

    a.active_power_kw,
    a.power_reactive_kvar,
    a.power_input_kw,
    a.power_factor,
    a.daily_active_energy_kwh,
    a.cumulative_active_energy_kwh,

    -- ✅ NOVO: potência DC (vem do stg_inverter_analog)
    a.power_dc_kw,

    case when a.active_power_kw > 0 then true else false end as is_generating,

    a.frequency_hz,
    a.efficiency_pct,

    a.current_phase_a_a,
    a.current_phase_b_a,
    a.current_phase_c_a,
    a.line_voltage_ab_v,
    a.line_voltage_bc_v,
    a.line_voltage_ca_v,
    a.string_voltage_v,

    a.temperature_internal_c,
    a.resistance_insulation_mohm,
    a.state_operation,

    -- ✅ status do inversor (diagnóstico/UI)
    s.working_status,
    s.inverter_status,

    -- ✅ compat (mantém a coluna antiga com o código cru)
    s.communication_fault_code as communication_fault,

    -- ✅ novo (use isso no MART)
    s.communication_fault_code,
    s.is_communication_ok

from analog a
left join status_dedup s
  on s.timestamp = a.timestamp
 and s.power_plant_id = a.power_plant_id
 and s.device_id = a.device_id

join {{ source('public','device') }} d
  on d.id = a.device_id
 and d.is_active = true
join {{ source('public','device_type') }} dt
  on dt.id = d.device_type_id
join {{ source('public','power_plant') }} pp
  on pp.id = a.power_plant_id
 and pp.is_active = true
