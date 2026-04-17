{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp','device_id']
) }}
with base as (
    select
        m.timestamp,
        m.power_plant_id,
        m.device_id,
        m.json_data
    from {{ source('public','raw_meter') }} m
    inner join public.device d
        on d.id = m.device_id
       and d.is_active is true
    {% if is_incremental() %}
      where m.timestamp > (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz)
        from {{ this }}
      )
    {% endif %}
)
select
    timestamp,
    power_plant_id,
    device_id,
    -- TENSÕES
    nullif(json_data ->> 'volt_uab_line', '')::numeric      as voltage_ab_v,
    nullif(json_data ->> 'volt_ubc_line', '')::numeric      as voltage_bc_v,
    nullif(json_data ->> 'volt_uca_line', '')::numeric      as voltage_ca_v,
    -- CORRENTES
    nullif(json_data ->> 'current_a_phase_a', '')::numeric  as current_a_a,
    nullif(json_data ->> 'current_b_phase_b', '')::numeric  as current_b_a,
    nullif(json_data ->> 'current_c_phase_c', '')::numeric  as current_c_a,
    -- POTÊNCIAS
    nullif(json_data ->> 'active_power', '')::numeric       as active_power_kw,
    nullif(json_data ->> 'apparent_power', '')::numeric     as apparent_power_kva,
    nullif(json_data ->> 'react_power', '')::numeric        as reactive_power_kvar,
    -- ELÉTRICAS
    nullif(json_data ->> 'power_factor', '')::numeric       as power_factor,
    nullif(json_data ->> 'frequency', '')::numeric          as frequency_hz,
    -- ENERGIAS
    nullif(json_data ->> 'energy_imp', '')::numeric         as energy_import_kwh,
    nullif(json_data ->> 'energy_exp', '')::numeric         as energy_export_kwh
from base
