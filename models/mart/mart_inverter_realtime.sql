{{ config(materialized = 'view') }}

with devices as (
    -- ✅ 1 linha por INVERSOR ATIVO (fonte da verdade para "total")
    select
        pp.customer_id,
        pp.id   as power_plant_id,
        pp.name as power_plant_name,
        d.id    as device_id,
        d.name  as device_name
    from {{ source('public','power_plant') }} pp
    join {{ source('public','device') }} d
      on d.power_plant_id = pp.id
     and d.is_active = true
    join {{ source('public','device_type') }} dt
      on dt.id = d.device_type_id
     and dt.name = 'inverter'
),

base as (
    -- leituras do inverter (pode ter vários registros por device)
    select
        a.*,
        pp.customer_id
    from {{ ref('int_inverter_analog') }} a
    join {{ source('public','power_plant') }} pp
      on pp.id = a.power_plant_id
),

last_per_inverter as (
    -- ✅ pega o último timestamp por inverter
    select
        customer_id,
        power_plant_id,
        device_id,
        max(timestamp) as last_ts
    from base
    group by 1,2,3
),

snap as (
    -- ✅ snapshot da última leitura (pode ficar vazio para inverter sem dado)
    select
        b.*
    from base b
    join last_per_inverter l
      on l.customer_id    = b.customer_id
     and l.power_plant_id = b.power_plant_id
     and l.device_id      = b.device_id
     and l.last_ts        = b.timestamp
)

select
    dv.customer_id,
    dv.power_plant_id,
    dv.power_plant_name,

    dv.device_id,
    dv.device_name as inverter_name,

<<<<<<< Updated upstream
    -- =============================
    -- ✅ COLUNAS PRINCIPAIS (UI)
    -- =============================
=======
    -- ======================================================
    -- COLUNAS UI (se não tem dado, zera)
    -- ======================================================
>>>>>>> Stashed changes
    coalesce(sn.active_power_kw, 0) as power_kw,

    coalesce(
      sn.efficiency_pct,
      case
        when sn.power_input_kw > 0 then round((sn.active_power_kw / sn.power_input_kw) * 100.0, 1)
        else null
      end
    ) as efficiency_pct,

    sn.temperature_internal_c as temp_c,
    sn.frequency_hz           as freq_hz,

    null::numeric as pr,

    sn.timestamp as last_reading_ts,

<<<<<<< Updated upstream
    -- =============================
    -- ✅ EXTRAS (DETALHES DO INVERSOR)
    -- (mantém NULL quando não há dado)
    -- =============================

    -- Potências / PF / energia
    sn.power_factor                 as power_factor,
    sn.power_reactive_kvar          as power_reactive_kvar,
    sn.power_input_kw               as power_input_kw,

    sn.daily_active_energy_kwh      as daily_active_energy_kwh,
    sn.cumulative_active_energy_kwh as cumulative_active_energy_kwh,

    -- Correntes AC
    sn.current_phase_a_a            as current_phase_a_a,
    sn.current_phase_b_a            as current_phase_b_a,
    sn.current_phase_c_a            as current_phase_c_a,

    -- Tensões AC (linha)
    sn.line_voltage_ab_v            as line_voltage_ab_v,
    sn.line_voltage_bc_v            as line_voltage_bc_v,
    sn.line_voltage_ca_v            as line_voltage_ca_v,

    -- DC
    sn.string_voltage_v             as string_voltage_v,
    sn.power_dc_kw                  as power_dc_kw,

    -- Isolação (MΩ)
    sn.resistance_insulation_mohm   as resistance_insulation_mohm,

    -- Status cru (debug)
    sn.working_status               as working_status,
    sn.inverter_status              as inverter_status,
    sn.state_operation              as state_operation,

    -- Comunicação
    sn.communication_fault_code     as communication_fault_code,
    sn.is_communication_ok          as is_communication_ok,

    -- =============================
    -- ✅ STATUS (CHIPS)
    -- 0 OFFLINE (No comm), 1 STANDBY/OFF, 2 RUNNING, 3 FAULT
    -- =============================
    case
      when sn.timestamp is null then 0
      when now() - sn.timestamp > interval '15 minutes' then 0

      -- ✅ por state_operation quando existir
      when sn.state_operation = 16 then 2                 -- RUNNING
      when sn.state_operation = 2  then 3                 -- FAULT
      when sn.state_operation in (0, 1) then 1            -- STANDBY/OFF
=======
    -- ======================================================
    -- ✅ NOVO: POTÊNCIA APARENTE (kVA) — vem do INT
    -- ======================================================
    sn.apparent_power_kva as apparent_power_kva,

    -- ======================================================
    -- STATUS CODE:
    -- 0 OFFLINE (No comm), 1 STANDBY/OFF, 2 RUNNING, 3 FAULT
    -- ======================================================
    case
      when sn.timestamp is null then 0
      when now() - sn.timestamp > interval '8 minutes' then 0

      when sn.state_operation = 16 then 2
      when sn.state_operation = 2  then 3
      when sn.state_operation in (0, 1) then 1
>>>>>>> Stashed changes

      when coalesce(sn.active_power_kw, 0) > 0.1 then 2
      else 1
    end as inverter_status_code,

    case
      when sn.timestamp is null then 'OFFLINE'
      when now() - sn.timestamp > interval '15 minutes' then 'OFFLINE'
      when sn.state_operation = 16 then 'RUNNING'
      when sn.state_operation = 2  then 'FAULT'
      when sn.state_operation in (0, 1) then 'STANDBY'
      when coalesce(sn.active_power_kw, 0) > 0.1 then 'RUNNING'
      else 'STANDBY'
    end as status

from devices dv
left join snap sn
  on sn.customer_id = dv.customer_id
 and sn.power_plant_id = dv.power_plant_id
 and sn.device_id = dv.device_id

order by dv.device_id