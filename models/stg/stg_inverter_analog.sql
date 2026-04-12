{{ config(
    materialized = 'incremental',
    unique_key = ['timestamp', 'device_id'],
    on_schema_change = 'sync_all_columns'
) }}

with base as (

    select
        r.timestamp as ingested_at,
        r.power_plant_id,
        r.device_id,
        r.json_data
    from {{ source('public', 'raw_inverter') }} r
    inner join public.device d
        on r.device_id = d.id
    where coalesce(d.is_active, false) = true

    {% if is_incremental() %}
      and r.timestamp >= (
        select coalesce(max(timestamp), '1970-01-01'::timestamptz) - interval '5 minutes'
        from {{ this }}
      )
    {% endif %}

),

dedup as (

    select
        *,
        row_number() over (
            partition by
                ingested_at,
                device_id
            order by ingested_at
        ) as rn
    from base
),

final as (

select
    ingested_at as timestamp,

    case
      when nullif(trim(json_data ->> 'timestamp'), '') is null then null
      else (json_data ->> 'timestamp')::timestamptz
    end as device_timestamp,

    power_plant_id,
    device_id,

    case
      when nullif(trim(json_data ->> 'active_power'), '') is null then null
      when replace(trim(json_data ->> 'active_power'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'active_power') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'active_power'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'active_power'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'active_power'), ',', '.')::numeric
      else null
    end as active_power_kw,

    case
      when nullif(trim(json_data ->> 'reactive_power'), '') is null then null
      when regexp_replace(replace(trim(json_data ->> 'reactive_power'), ',', '.'), '^0\.-', '-') ~ '.*[0-9]+-[0-9]+.*'
        then null
      when regexp_replace(replace(trim(json_data ->> 'reactive_power'), ',', '.'), '^0\.-', '-') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then regexp_replace(replace(trim(json_data ->> 'reactive_power'), ',', '.'), '^0\.-', '-')::numeric
      else null
    end as power_reactive_kvar,

    case
      when nullif(trim(json_data ->> 'apparent_power'), '') is null then null
      when replace(trim(json_data ->> 'apparent_power'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'apparent_power') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'apparent_power'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'apparent_power'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'apparent_power'), ',', '.')::numeric
      else null
    end as apparent_power_kva,

    case
      when nullif(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), '') is null then null
      when replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')) ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), '^0\.-', ''))::numeric
      when replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(coalesce(json_data ->> 'power_input', json_data ->> 'power_dc')), ',', '.')::numeric
      else null
    end as power_input_kw,

    case
      when nullif(trim(json_data ->> 'power_factor'), '') is null then null
      when replace(trim(json_data ->> 'power_factor'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'power_factor') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'power_factor'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'power_factor'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'power_factor'), ',', '.')::numeric
      else null
    end as power_factor,

    case
      when nullif(trim(json_data ->> 'total_daily_energy'), '') is null then null
      when replace(trim(json_data ->> 'total_daily_energy'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'total_daily_energy') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'total_daily_energy'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'total_daily_energy'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'total_daily_energy'), ',', '.')::numeric
      else null
    end as daily_active_energy_kwh,

    case
      when nullif(trim(json_data ->> 'total_energy'), '') is null then null
      when replace(trim(json_data ->> 'total_energy'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'total_energy') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'total_energy'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'total_energy'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'total_energy'), ',', '.')::numeric
      else null
    end as cumulative_active_energy_kwh,

    case
      when nullif(trim(json_data ->> 'frequency'), '') is null then null
      when replace(trim(json_data ->> 'frequency'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'frequency') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'frequency'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'frequency'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'frequency'), ',', '.')::numeric
      else null
    end as frequency_hz,

    case
      when nullif(trim(json_data ->> 'efficiency'), '') is null then null
      when replace(trim(json_data ->> 'efficiency'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'efficiency') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'efficiency'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'efficiency'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'efficiency'), ',', '.')::numeric
      else null
    end as efficiency_pct,

    case
      when nullif(trim(json_data ->> 'current_ac_phase_a'), '') is null then null
      when replace(trim(json_data ->> 'current_ac_phase_a'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'current_ac_phase_a') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'current_ac_phase_a'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'current_ac_phase_a'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'current_ac_phase_a'), ',', '.')::numeric
      else null
    end as current_phase_a_a,

    case
      when nullif(trim(json_data ->> 'current_ac_phase_b'), '') is null then null
      when replace(trim(json_data ->> 'current_ac_phase_b'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'current_ac_phase_b') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'current_ac_phase_b'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'current_ac_phase_b'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'current_ac_phase_b'), ',', '.')::numeric
      else null
    end as current_phase_b_a,

    case
      when nullif(trim(json_data ->> 'current_ac_phase_c'), '') is null then null
      when replace(trim(json_data ->> 'current_ac_phase_c'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'current_ac_phase_c') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'current_ac_phase_c'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'current_ac_phase_c'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'current_ac_phase_c'), ',', '.')::numeric
      else null
    end as current_phase_c_a,

    case
      when nullif(trim(json_data ->> 'voltage_ac_phase_ab'), '') is null then null
      when replace(trim(json_data ->> 'voltage_ac_phase_ab'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'voltage_ac_phase_ab') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_ac_phase_ab'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_ac_phase_ab'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_ac_phase_ab'), ',', '.')::numeric
      else null
    end as line_voltage_ab_v,

    case
      when nullif(trim(json_data ->> 'voltage_ac_phase_bc'), '') is null then null
      when replace(trim(json_data ->> 'voltage_ac_phase_bc'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'voltage_ac_phase_bc') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_ac_phase_bc'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_ac_phase_bc'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_ac_phase_bc'), ',', '.')::numeric
      else null
    end as line_voltage_bc_v,

    case
      when nullif(trim(json_data ->> 'voltage_ac_phase_ca'), '') is null then null
      when replace(trim(json_data ->> 'voltage_ac_phase_ca'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'voltage_ac_phase_ca') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_ac_phase_ca'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_ac_phase_ca'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_ac_phase_ca'), ',', '.')::numeric
      else null
    end as line_voltage_ca_v,

    case
      when nullif(trim(json_data ->> 'voltage_dc'), '') is null then null
      when replace(trim(json_data ->> 'voltage_dc'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'voltage_dc') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'voltage_dc'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'voltage_dc'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'voltage_dc'), ',', '.')::numeric
      else null
    end as string_voltage_v,

    case
      when nullif(trim(json_data ->> 'power_dc'), '') is null then null
      when replace(trim(json_data ->> 'power_dc'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'power_dc') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'power_dc'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'power_dc'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'power_dc'), ',', '.')::numeric
      else null
    end as power_dc_kw,

    case
      when nullif(trim(json_data ->> 'temperature_current'), '') is null then null
      when replace(trim(json_data ->> 'temperature_current'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'temperature_current') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'temperature_current'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'temperature_current'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'temperature_current'), ',', '.')::numeric
      else null
    end as temperature_internal_c,

    case
      when nullif(trim(json_data ->> 'isolation'), '') is null then null
      when replace(trim(json_data ->> 'isolation'), ',', '.') ~ '.*[0-9]+-[0-9]+.*' then null
      when trim(json_data ->> 'isolation') ~ '^0\.-[0-9]+(\.[0-9]+)?$'
        then ('-' || regexp_replace(trim(json_data ->> 'isolation'), '^0\.-', ''))::numeric
      when replace(trim(json_data ->> 'isolation'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
        then replace(trim(json_data ->> 'isolation'), ',', '.')::numeric
      else null
    end as resistance_insulation_mohm,

    case
      when (json_data ->> 'working_status') ~ '^[0-9]+$'
      then (json_data ->> 'working_status')::int
      else null
    end as working_status,

    case
      when coalesce(json_data ->> 'inverter_status', json_data ->> 'Inverter_status') ~ '^[0-9]+$'
      then coalesce(json_data ->> 'inverter_status', json_data ->> 'Inverter_status')::int
      else null
    end as state_operation

from dedup
where rn = 1
)

select *
from final