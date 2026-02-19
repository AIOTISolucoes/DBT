{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'device_id']
) }}

with base as (

    select
        r.timestamp,
        r.power_plant_id,
        r.device_id,
        r.json_data
    from {{ source('public', 'raw_inverter') }} r

    {% if is_incremental() %}
      where r.timestamp >= (
        select coalesce(max(timestamp), '1970-01-01') - interval '5 minutes'
        from {{ this }}
      )
    {% endif %}

),

dedup as (

    select
        *,
        row_number() over (
            partition by
                timestamp,
                device_id
            order by timestamp
        ) as rn
    from base
),

final as (

select
    timestamp,
    power_plant_id,
    device_id,

    -- POTÊNCIAS
    case
      when nullif(trim(json_data ->> 'active_power'), '') is null then null
      when trim(json_data ->> 'active_power') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'active_power'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'active_power'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'active_power'), ',', '.')::numeric
      else null
    end as active_power_kw,

    case
      when nullif(trim(json_data ->> 'reactive_power'), '') is null then null
      when trim(json_data ->> 'reactive_power') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'reactive_power'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'reactive_power'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'reactive_power'), ',', '.')::numeric
      else null
    end as power_reactive_kvar,

    -- ✅ AJUSTE: power_input_kw agora aceita power_input OU power_dc
    case
      when nullif(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), '') is null then null
      when trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')) ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), '^0\.-', ''))::numeric
      when replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), ',', '.')::numeric
      else null
    end as power_input_kw,

    case
      when nullif(trim(json_data ->> 'power_factor'), '') is null then null
      when trim(json_data ->> 'power_factor') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'power_factor'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'power_factor'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'power_factor'), ',', '.')::numeric
      else null
    end as power_factor,

    -- ENERGIA
    case
      when nullif(trim(json_data ->> 'total_daily_energy'), '') is null then null
      when trim(json_data ->> 'total_daily_energy') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'total_daily_energy'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'total_daily_energy'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'total_daily_energy'), ',', '.')::numeric
      else null
    end as daily_active_energy_kwh,

    case
      when nullif(trim(json_data ->> 'total_energy'), '') is null then null
      when trim(json_data ->> 'total_energy') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'total_energy'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'total_energy'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'total_energy'), ',', '.')::numeric
      else null
    end as cumulative_active_energy_kwh,

    -- FREQUÊNCIA / EFICIÊNCIA
    case
      when nullif(trim(json_data ->> 'frequency'), '') is null then null
      when trim(json_data ->> 'frequency') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'frequency'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'frequency'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'frequency'), ',', '.')::numeric
      else null
    end as frequency_hz,

    -- ✅ vem do payload (efficiency)
    case
      when nullif(trim(json_data ->> 'efficiency'), '') is null then null
      when trim(json_data ->> 'efficiency') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'efficiency'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'efficiency'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'efficiency'), ',', '.')::numeric
      else null
    end as efficiency_pct,

    -- CORRENTES AC
    case
      when nullif(trim(json_data ->> 'current_ac_phase_a'), '') is null then null
      when trim(json_data ->> 'current_ac_phase_a') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'current_ac_phase_a'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'current_ac_phase_a'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'current_ac_phase_a'), ',', '.')::numeric
      else null
    end as current_phase_a_a,

    case
      when nullif(trim(json_data ->> 'current_ac_phase_b'), '') is null then null
      when trim(json_data ->> 'current_ac_phase_b') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'current_ac_phase_b'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'current_ac_phase_b'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'current_ac_phase_b'), ',', '.')::numeric
      else null
    end as current_phase_b_a,

    case
      when nullif(trim(json_data ->> 'current_ac_phase_c'), '') is null then null
      when trim(json_data ->> 'current_ac_phase_c') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'current_ac_phase_c'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'current_ac_phase_c'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'current_ac_phase_c'), ',', '.')::numeric
      else null
    end as current_phase_c_a,

    -- TENSÕES AC (LINHA)
    case
      when nullif(trim(json_data ->> 'voltage_ac_phase_ab'), '') is null then null
      when trim(json_data ->> 'voltage_ac_phase_ab') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_ac_phase_ab'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_ac_phase_ab'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_ac_phase_ab'), ',', '.')::numeric
      else null
    end as line_voltage_ab_v,

    case
      when nullif(trim(json_data ->> 'voltage_ac_phase_bc'), '') is null then null
      when trim(json_data ->> 'voltage_ac_phase_bc') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_ac_phase_bc'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_ac_phase_bc'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_ac_phase_bc'), ',', '.')::numeric
      else null
    end as line_voltage_bc_v,

    case
      when nullif(trim(json_data ->> 'voltage_ac_phase_ca'), '') is null then null
      when trim(json_data ->> 'voltage_ac_phase_ca') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_ac_phase_ca'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_ac_phase_ca'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_ac_phase_ca'), ',', '.')::numeric
      else null
    end as line_voltage_ca_v,

    -- DC
    case
      when nullif(trim(json_data ->> 'voltage_dc'), '') is null then null
      when trim(json_data ->> 'voltage_dc') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_dc'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_dc'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_dc'), ',', '.')::numeric
      else null
    end as string_voltage_v,

    case
      when nullif(trim(json_data ->> 'power_dc'), '') is null then null
      when trim(json_data ->> 'power_dc') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'power_dc'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'power_dc'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'power_dc'), ',', '.')::numeric
      else null
    end as power_dc_kw,

    -- TEMPERATURA / ISOLAÇÃO
    case
      when nullif(trim(json_data ->> 'temperature_current'), '') is null then null
      when trim(json_data ->> 'temperature_current') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'temperature_current'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'temperature_current'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'temperature_current'), ',', '.')::numeric
      else null
    end as temperature_internal_c,

    case
      when nullif(trim(json_data ->> 'isolation'), '') is null then null
      when trim(json_data ->> 'isolation') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'isolation'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'isolation'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'isolation'), ',', '.')::numeric
      else null
    end as resistance_insulation_mohm,

    -- WORKING STATUS
    case
      when (json_data ->> 'working_status') ~ '^[0-9]+$'
      then (json_data ->> 'working_status')::int
      else null
    end as working_status,

    -- ESTADO OPERACIONAL
    case
      when (json_data ->> 'inverter_status') ~ '^[0-9]+$'
      then (json_data ->> 'inverter_status')::int
      else null
    end as state_operation

from dedup
where rn = 1
)

select *
from final
